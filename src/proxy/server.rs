use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use log::{debug, error, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::acl::domain_blocker::DomainBlocker;
use crate::config::Config;
use crate::error::{ProxyError, Result};
use crate::logging::Logger;
use crate::metrics::Metrics;
use crate::proxy::http_utils::{find_header_end, log_http_response, get_timeout_duration, 
                               extract_status_code, is_response_complete, get_buffer_size,
                               HttpRequestInfo, parse_http_request_basic, log_http_request,
                               send_client_request_to_server};

/// HTTP 세션 처리 구조체
pub struct HttpSession {
    client_stream: TcpStream,
    server_stream: TcpStream,
    session_id: String,
    config: Option<Arc<Config>>,
    logger: Option<Arc<Logger>>,
    domain_blocker: Option<Arc<DomainBlocker>>,
    metrics: Arc<Metrics>,
    client_buf: BytesMut,
    server_buf: BytesMut,
    start_time: Instant,
}

impl HttpSession {
    /// 새로운 HTTP 세션 생성
    pub fn new(
        client_stream: TcpStream,
        server_stream: TcpStream,
        session_id: String,
        config: Option<Arc<Config>>,
        logger: Option<Arc<Logger>>,
        metrics: Arc<Metrics>,
        domain_blocker: Option<Arc<DomainBlocker>>,
    ) -> Self {
        let buffer_size = get_buffer_size(&config);
        
        Self {
            client_stream,
            server_stream,
            session_id,
            config,
            logger,
            domain_blocker,
            metrics,
            client_buf: BytesMut::with_capacity(buffer_size),
            server_buf: BytesMut::with_capacity(buffer_size),
            start_time: Instant::now(),
        }
    }
    
    /// 클라이언트로부터 초기 요청 읽기 - 최적화 버전
    async fn read_initial_request(&mut self) -> Result<HttpRequestInfo> {
        let timeout_duration = get_timeout_duration(&self.config);
        
        // 클라이언트로부터 초기 요청 읽기
        loop {
            match tokio::time::timeout(timeout_duration, self.client_stream.read_buf(&mut self.client_buf)).await {
                Ok(result) => {
                    match result {
                        Ok(0) => {
                            return Err(ProxyError::Http("클라이언트가 연결을 닫음".to_string()));
                        }
                        Ok(n) => {
                            debug!("[Session:{}] 클라이언트로부터 {}바이트 수신", self.session_id, n);
                            
                            // 헤더 끝 위치 확인
                            if let Some(header_end_pos) = find_header_end(&self.client_buf) {
                                // 요청 헤더 파싱
                                let headers = String::from_utf8_lossy(&self.client_buf[..header_end_pos]);
                                let request_info = parse_http_request_basic(&headers);
                                
                                // 도메인 차단 확인
                                if let Some(domain_blocker) = &self.domain_blocker {
                                    if domain_blocker.is_blocked(&request_info.host) {
                                        error!("[Session:{}] 차단된 도메인 접근 시도: {}", self.session_id, request_info.host);
                                        return Err(ProxyError::Http(format!("차단된 도메인 접근: {}", request_info.host)));
                                    }
                                }
                                
                                // 요청 로깅
                                if let Some(logger) = &self.logger {
                                    log_http_request(
                                        logger,
                                        &self.session_id,
                                        &self.client_buf,
                                        header_end_pos,
                                        &request_info
                                    ).await?;
                                }
                                
                                return Ok(request_info);
                            }
                        }
                        Err(e) => {
                            return Err(ProxyError::Http(format!("클라이언트로부터 읽기 오류: {}", e)));
                        }
                    }
                }
                Err(_) => {
                    return Err(ProxyError::Http("클라이언트 읽기 타임아웃".to_string()));
                }
            }
        }
    }
    
    /// 서버에 요청 전송
    async fn send_request_to_server(&mut self) -> Result<()> {
        send_client_request_to_server(&self.client_buf, &mut self.server_stream, &self.session_id).await?;
        
        // 버퍼 비우기
        self.client_buf.clear();
        
        Ok(())
    }
    
    /// HTTP 세션 처리 - 전체 프로세스 관리
    pub async fn process(&mut self) -> Result<()> {
        debug!("[Session:{}] HTTP 세션 처리 시작", self.session_id);
        
        // 시작 시간 기록 및 메트릭 업데이트
        self.metrics.increment_http_session_count();
        
        // 클라이언트로부터 초기 요청 읽기
        let request_info = match self.read_initial_request().await {
            Ok(info) => {
                debug!("[Session:{}] 초기 요청 읽기 성공 - 호스트: {}, 메서드: {}", 
                       self.session_id, info.host, info.method);
                
                // 메트릭에 요청 정보 기록
                self.metrics.increment_http_request_count();
                if info.method == "GET" {
                    self.metrics.increment_get_request_count();
                } else if info.method == "POST" {
                    self.metrics.increment_post_request_count();
                }
                
                info
            },
            Err(e) => {
                // 연결 종료 관련 오류는 정상 처리로 간주
                if let ProxyError::Http(err_msg) = &e {
                    if err_msg.contains("ConnectionReset") || 
                       err_msg.contains("ConnectionAborted") || 
                       err_msg.contains("BrokenPipe") || 
                       err_msg.contains("클라이언트가 연결을 닫음") {
                        debug!("[Session:{}] 초기 요청 읽기 중 정상 연결 종료: {}", self.session_id, e);
                        return Ok(());
                    }
                }
                
                error!("[Session:{}] 초기 요청 처리 실패: {}", self.session_id, e);
                self.metrics.increment_error_count();
                return Err(e);
            }
        };
        
        // 서버에 요청 전송
        if let Err(e) = self.send_request_to_server().await {
            error!("[Session:{}] 서버에 요청 전송 실패: {}", self.session_id, e);
            self.metrics.increment_error_count();
            return Err(e);
        }
        
        debug!("[Session:{}] 서버에 요청 전송 완료", self.session_id);
        
        // 서버 응답 처리
        match self.process_server_response().await {
            Ok(_) => {
                debug!("[Session:{}] 서버 응답 처리 완료", self.session_id);
                // 성공적인 세션 완료 메트릭 업데이트
                let response_time = self.start_time.elapsed().as_millis() as u64;
                self.metrics.record_response_time(response_time);
                Ok(())
            },
            Err(e) => {
                // 연결 종료 관련 오류는 정상 처리로 간주
                if let ProxyError::Http(err_msg) = &e {
                    if err_msg.contains("ConnectionReset") || 
                       err_msg.contains("ConnectionAborted") || 
                       err_msg.contains("BrokenPipe") {
                        debug!("[Session:{}] 서버 응답 처리 중 정상 연결 종료: {}", self.session_id, e);
                        return Ok(());
                    }
                }
                
                error!("[Session:{}] 서버 응답 처리 실패: {}", self.session_id, e);
                self.metrics.increment_error_count();
                Err(e)
            }
        }
    }
    
    /// 서버로부터 응답 처리 - 개선된 버전
    async fn process_server_response(&mut self) -> Result<()> {
        // 응답 처리 시작 시간 기록
        let start = Instant::now();
        let session_id = self.session_id.clone();
        let timeout_duration = get_timeout_duration(&self.config);
        let mut header_end_pos = 0; // 헤더 끝 위치 추적을 위한 변수
        
        // 헤더를 찾기 위해 초기 데이터 읽기
        debug!("[Session:{}] 서버로부터 초기 응답 읽기 시작", session_id);
        match tokio::time::timeout(timeout_duration, self.server_stream.read_buf(&mut self.server_buf)).await {
            Ok(result) => {
                match result {
                    Ok(0) => {
                        debug!("[Session:{}] 서버가 연결을 닫음, 응답 없음", session_id);
                        return Ok(());
                    }
                    Ok(n) => {
                        debug!("[Session:{}] 서버로부터 초기 {}바이트 수신 ({:?})", 
                               session_id, n, start.elapsed());
                        
                        // 헤더 파싱 - 헤더 끝(

                        if let Some(end_pos) = find_header_end(&self.server_buf) {
                            header_end_pos = end_pos;
                            self.metrics.increment_http_response_count();
                            
                            // 응답 헤더 분석
                            let headers = &self.server_buf[..end_pos];
                            let headers_str = String::from_utf8_lossy(headers);
                            if let Some(status_code) = extract_status_code(&headers_str) {
                                debug!("[Session:{}] 응답 상태 코드: {}", session_id, status_code);
                                
                                // 상태 코드에 따른 특별 처리
                                if status_code >= 400 {
                                    warn!("[Session:{}] 오류 응답 받음: {}", session_id, status_code);
                                    self.metrics.increment_error_count();
                                }
                            }
                        } else {
                            // 헤더를 찾지 못한 경우
                            warn!("[Session:{}] 헤더 끝을 찾지 못함, 추정하여 계속 진행", session_id);
                            header_end_pos = 0; // 기본값 사용
                        }
                        
                        // 로깅을 위해 헤더 정보 저장
                        let response_time = self.start_time.elapsed().as_millis() as u64;
                        let server_buf_clone = self.server_buf.clone();
                        
                        // 비동기 로깅 작업 실행
                        if let Some(logger) = &self.logger {
                            let logger_clone = logger.clone();
                            let session_id_clone = session_id.clone();
                            tokio::spawn(async move {
                                if let Err(e) = log_http_response(
                                    &logger_clone,
                                    &session_id_clone,
                                    &server_buf_clone,
                                    header_end_pos,
                                    response_time
                                ).await {
                                    error!("[Session:{}] 응답 로깅 실패: {}", session_id_clone, e);
                                }
                            });
                        }
                        
                        // 초기 데이터 클라이언트에 전송
                        if let Err(e) = self.client_stream.write_all(&self.server_buf).await {
                            if e.kind() == std::io::ErrorKind::ConnectionReset || 
                               e.kind() == std::io::ErrorKind::ConnectionAborted || 
                               e.kind() == std::io::ErrorKind::BrokenPipe {
                                debug!("[Session:{}] 클라이언트 연결 정상 종료 (초기 응답 전송 중): {}", session_id, e);
                                return Ok(());
                            } else {
                                error!("[Session:{}] 초기 응답 전송 실패: {}", session_id, e);
                                return Err(ProxyError::Http(format!("클라이언트에 응답 전송 실패: {}", e)));
                            }
                        }
                        
                        // 데이터가 확실히 전송되도록 flush 수행
                        if let Err(e) = self.client_stream.flush().await {
                            if e.kind() == std::io::ErrorKind::ConnectionReset || 
                               e.kind() == std::io::ErrorKind::ConnectionAborted || 
                               e.kind() == std::io::ErrorKind::BrokenPipe {
                                debug!("[Session:{}] 클라이언트 연결 정상 종료 (flush 중): {}", session_id, e);
                                return Ok(());
                            } else {
                                error!("[Session:{}] 초기 응답 flush 실패: {}", session_id, e);
                                return Err(ProxyError::Http(format!("클라이언트에 응답 flush 실패: {}", e)));
                            }
                        }
                        
                        // 응답이 이미 완료되었는지 확인
                        if is_response_complete(&self.server_buf, header_end_pos, &session_id) {
                            debug!("[Session:{}] 초기 응답으로 완료 감지, 추가 데이터 읽기 생략", session_id);
                            return Ok(());
                        }
                    }
                    Err(e) => {
                        error!("[Session:{}] 서버로부터 초기 읽기 오류: {}", session_id, e);
                        return Err(ProxyError::Http(format!("서버로부터 읽기 오류: {}", e)));
                    }
                }
            }
            Err(_) => {
                warn!("[Session:{}] 서버 초기 읽기 타임아웃", session_id);
                return Err(ProxyError::Http("서버 초기 읽기 타임아웃".to_string()));
            }
        }
        
        // 나머지 데이터 처리 - 설정에서 버퍼 크기 가져오기
        let buffer_size = match &self.config {
            Some(config) => config.buffer_size,
            None => crate::constants::DEFAULT_BUFFER_SIZE,
        };
        
        // 메모리 사용량 최적화: 임시 버퍼를 사용하지 않고 직접 할당
        debug!("[Session:{}] 추가 응답 처리를 위한 버퍼 할당 (크기: {})", session_id, buffer_size);
        let mut buf = BytesMut::with_capacity(buffer_size);
        buf.resize(buffer_size, 0);
        
        // 응답 처리 통계 변수
        let mut total_bytes = self.server_buf.len();
        let mut chunks_count = 1; // 초기 청크 포함
        
        // 서버로부터 응답 데이터 계속 읽기
        loop {
            // 서버로부터 데이터 읽기
            let read_start = Instant::now();
            let n = match tokio::time::timeout(timeout_duration, self.server_stream.read(&mut buf[..buffer_size])).await {
                Ok(result) => match result {
                    Ok(0) => {
                        debug!("[Session:{}] 서버가 연결을 닫음 (데이터 전송 완료)", session_id);
                        break; // 서버가 연결을 닫음
                    }
                    Ok(n) => {
                        total_bytes += n;
                        chunks_count += 1;
                        debug!("[Session:{}] 서버로부터 {}바이트 추가 수신 ({:?})", 
                               session_id, n, read_start.elapsed());
                        n
                    },
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::ConnectionReset || 
                           e.kind() == std::io::ErrorKind::ConnectionAborted || 
                           e.kind() == std::io::ErrorKind::BrokenPipe {
                            debug!("[Session:{}] 서버 연결 정상 종료: {}", session_id, e);
                            break;
                        } else {
                            error!("[Session:{}] 서버로부터 읽기 오류: {}", session_id, e);
                            return Err(ProxyError::Http(format!("서버로부터 읽기 오류: {}", e)));
                        }
                    }
                },
                Err(_) => {
                    // 타임아웃이 발생했지만, 이미 일부 데이터를 처리했다면 정상 종료로 간주
                    debug!("[Session:{}] 서버 읽기 타임아웃, 응답 처리 완료로 간주", session_id);
                    break;
                }
            };
            
            // 클라이언트에 데이터 전송
            let write_start = Instant::now();
            if let Err(e) = self.client_stream.write_all(&buf[0..n]).await {
                if e.kind() == std::io::ErrorKind::ConnectionReset || 
                   e.kind() == std::io::ErrorKind::ConnectionAborted || 
                   e.kind() == std::io::ErrorKind::BrokenPipe {
                    debug!("[Session:{}] 클라이언트 연결 정상 종료: {}", session_id, e);
                    break;
                } else {
                    error!("[Session:{}] 클라이언트에 데이터 전송 실패: {}", session_id, e);
                    return Err(ProxyError::Http(format!("클라이언트에 데이터 전송 실패: {}", e)));
                }
            }
            
            // 데이터가 확실히 전송되도록 flush 수행
            if let Err(e) = self.client_stream.flush().await {
                if e.kind() == std::io::ErrorKind::ConnectionReset || 
                   e.kind() == std::io::ErrorKind::ConnectionAborted || 
                   e.kind() == std::io::ErrorKind::BrokenPipe {
                    debug!("[Session:{}] 클라이언트 연결 정상 종료 (flush 중): {}", session_id, e);
                    break;
                } else {
                    error!("[Session:{}] 클라이언트에 데이터 flush 실패: {}", session_id, e);
                    return Err(ProxyError::Http(format!("클라이언트에 데이터 flush 실패: {}", e)));
                }
            }
            debug!("[Session:{}] 데이터 전송 완료 ({:?})", session_id, write_start.elapsed());
        }
        
        // 응답 처리 통계 기록
        let total_time = self.start_time.elapsed();
        self.metrics.record_response_time(total_time.as_millis() as u64);
        debug!("[Session:{}] 응답 처리 완료: 총 {} 바이트, {} 청크, {:?}", 
               session_id, total_bytes, chunks_count, total_time);
        
        debug!("[Session:{}] 응답 처리 완료", self.session_id);
        
        // 서버에 요청 전송
        if let Err(e) = self.send_request_to_server().await {
            error!("[Session:{}] 서버에 요청 전송 실패: {}", self.session_id, e);
            self.metrics.increment_error_count();
            return Err(e);
        }
        
        debug!("[Session:{}] 서버에 요청 전송 완료", self.session_id);
        
        // 성공적인 세션 완료 메트릭 업데이트
        let response_time = self.start_time.elapsed().as_millis() as u64;
        self.metrics.record_response_time(response_time);
        debug!("[Session:{}] HTTP 세션 완료", self.session_id);
        Ok(())
    }
}


