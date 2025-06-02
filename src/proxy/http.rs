use std::error::Error;
use std::sync::Arc;
use std::io;
use std::time::{Instant, Duration};

use log::{debug, error, info, warn};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::{BytesMut, BufMut};

use crate::metrics::Metrics;
use crate::constants::*;
use crate::REQUEST_LOGGER;
use crate::config::Config;

/// HTTP 헤더 끝 찾기
fn find_header_end(buf: &[u8]) -> Option<usize> {
    for i in 0..buf.len().saturating_sub(3) {
        if buf[i] == b'\r' && buf[i+1] == b'\n' && buf[i+2] == b'\r' && buf[i+3] == b'\n' {
            return Some(i);
        }
    }
    None
}

/// HTTP chunked 응답 종료 확인
fn find_chunk_end(buf: &[u8]) -> bool {
    // 마지막 청크 패턴: "\r\n0\r\n\r\n"
    let pattern = b"\r\n0\r\n\r\n";
    
    // 버퍼의 마지막 부분만 검사 (효율성을 위해)
    let start_pos = buf.len().saturating_sub(100);
    let slice_to_check = &buf[start_pos..];
    
    for i in 0..slice_to_check.len().saturating_sub(pattern.len() - 1) {
        let mut found = true;
        for j in 0..pattern.len() {
            if i + j >= slice_to_check.len() || slice_to_check[i + j] != pattern[j] {
                found = false;
                break;
            }
        }
        if found {
            return true;
        }
    }
    
    // 또는 단순히 끝나는 패턴을 직접 확인
    let end = buf.len().saturating_sub(5);
    end >= 5 && &buf[end..] == b"0\r\n\r\n"
}

/// HTTP 요청의 기본 정보를 파싱하는 함수
/// 메서드, 경로, 호스트, 헤더 종료 위치를 반환
fn parse_http_request_basic(request: &str) -> (Option<&str>, Option<&str>, Option<&str>, Option<usize>) {
    let mut method = None;
    let mut path = None;
    let mut host = None;
    let mut header_end = None;
    
    // 헤더 종료 위치 찾기
    if let Some(end) = request.find("\r\n\r\n") {
        header_end = Some(end);
    }
    
    // 첫 줄 파싱
    if let Some(first_line_end) = request.find("\r\n") {
        let first_line = &request[..first_line_end];
        let parts: Vec<&str> = first_line.split_whitespace().collect();
        
        if parts.len() >= 2 {
            method = Some(parts[0]);
            path = Some(parts[1]);
        }
    }
    
    // 호스트 헤더 찾기
    for line in request.lines() {
        if line.to_lowercase().starts_with("host:") {
            host = Some(line.trim_start_matches("Host:").trim_start_matches("host:").trim());
            break;
        }
    }
    
    (method, path, host, header_end)
}

/// 간소화된 HTTP 프록시 함수
pub async fn proxy_http_streams(
    mut client_stream: TcpStream,
    mut server_stream: TcpStream,
    metrics: Arc<Metrics>,
    session_id: &str,
    _request_start_time: Instant,
    config: Option<Arc<Config>>,
    initial_request: Option<Vec<u8>>,
    already_logged: bool,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // 세션 ID를 문자열로 복제하여 일관된 사용 보장
    let session_id_str = session_id.to_string();
    
    // 클라이언트 및 서버 IP 주소 가져오기
    let client_ip = client_stream.peer_addr()
        .map(|addr| addr.ip().to_string())
        .unwrap_or_else(|_| "Unknown IP".to_string());
    
    let server_ip = server_stream.peer_addr()
        .map(|addr| addr.ip().to_string())
        .unwrap_or_else(|_| "Unknown IP".to_string());
    
    // 타임아웃 설정
    let timeout_ms = if let Some(cfg) = &config {
        cfg.timeout_ms as u64
    } else {
        60000 // 기본값 60초
    };
    
    info!("[Session:{}] HTTP 프록시 시작 - 간소화 모드", session_id_str);
    
    // 버퍼 초기화
    let mut client_buf = BytesMut::with_capacity(BUFFER_SIZE_MEDIUM);
    
    // 초기 요청 데이터가 있으면 클라이언트 버퍼에 추가
    if let Some(initial_data) = initial_request {
        if !initial_data.is_empty() {
            client_buf.put_slice(&initial_data);
            info!("[Session:{}] 초기 요청 데이터 {} 바이트 사용", session_id_str, initial_data.len());
        }
    }
    
    // 초기 데이터가 없거나 추가 데이터가 필요한 경우에만 클라이언트로부터 요청을 읽음
    let mut total_bytes_in = client_buf.len();
    
    if total_bytes_in == 0 {
        // 초기 요청 데이터가 없는 경우에만 클라이언트에서 직접 읽음
        let mut buf = BytesMut::with_capacity(8192);
        match tokio::time::timeout(
            Duration::from_millis(timeout_ms),
            client_stream.read_buf(&mut buf)
        ).await {
            Ok(Ok(0)) => {
                info!("[Session:{}] 클라이언트 연결 종료됨", session_id_str);
                return Ok(());
            },
            Ok(Ok(n)) => {
                total_bytes_in += n;
                metrics.add_http_bytes_in(n as u64);
                client_buf.put_slice(&buf[..n]);
                debug!("[Session:{}] 클라이언트에서 {} 바이트 읽음", session_id_str, n);
            },
            Ok(Err(e)) => {
                error!("[Session:{}] 클라이언트 읽기 오류: {}", session_id_str, e);
                return Err(e.into());
            },
            Err(_) => {
                error!("[Session:{}] 클라이언트 읽기 타임아웃", session_id_str);
                return Err(io::Error::new(io::ErrorKind::TimedOut, "클라이언트 읽기 타임아웃").into());
            }
        }
    }
    
    if client_buf.is_empty() {
        info!("[Session:{}] 클라이언트로부터 데이터를 받지 못함", session_id_str);
        return Ok(());
    }
    
    info!("[Session:{}] 클라이언트 요청 {} 바이트 수신 완료", session_id_str, client_buf.len());
    
    // 요청 로깅
    if !already_logged {
        // 이미 로깅된 요청이 아닌 경우만 로깅 수행
        if let Ok(logger) = REQUEST_LOGGER.try_read() {
            // 요청 내용을 문자열로 변환 (헤더 부분만)
            let request_text = if let Ok(text) = std::str::from_utf8(&client_buf) {
                text.to_owned()
            } else {
                // 유효한 UTF-8이 아닌 경우 부분 변환 시도
                String::from_utf8_lossy(&client_buf).into_owned()
            };
            
            // 요청 파싱 - 메서드, 경로, 호스트 등
            let (method, path, host, header_end) = parse_http_request_basic(&request_text);
            
            let method = method.unwrap_or("UNKNOWN");
            let path = path.unwrap_or("/");
            let host = host.unwrap_or("unknown.host");
            
            // 헤더와 본문 분리
            let headers = request_text.split("\r\n\r\n").next().unwrap_or("");
            
            // 본문 추출 (있는 경우)
            let body = if let Some(end) = header_end {
                if end + 4 < request_text.len() {
                    Some(request_text[end + 4..].to_string())
                } else {
                    None
                }
            } else {
                None
            };
            
            // 브라우저 자동 요청 확인
            let is_auto_request = request_text.contains("GET /favicon.ico") || 
                                request_text.contains("GET /robots.txt") || 
                                request_text.contains("GET /.well-known") ||
                                request_text.contains("GET /apple-touch-icon");
            
            if !is_auto_request {
                // 자동 요청이 아닐 때만 로그 기록
                                                    if let Err(e) = logger.log_async(
                    host.to_string(),
                                                        method,
                                                        path,
                    headers.to_string(),
                    body,
                    &session_id_str,
                    &client_ip,
                    &server_ip,
                                                        None,
                                                        false,
                    false
                                                    ) {
                                                        warn!("[Session:{}] HTTP 요청 로깅 실패: {}", session_id_str, e);
                                                    } else {
                    debug!("[Session:{}] HTTP 요청 로깅 성공: {} {}", session_id_str, method, path);
                }
            } else {
                debug!("[Session:{}] 브라우저 자동 요청 감지됨 ({}) - 로깅 생략", session_id_str, path);
            }
        }
    } else {
        debug!("[Session:{}] 이미 로깅된 요청 - 중복 로깅 생략", session_id_str);
    }
    
    // 서버에 요청 전송
    info!("[Session:{}] 서버에 요청 전송 중 ({} 바이트)", session_id_str, client_buf.len());
    match server_stream.write_all(&client_buf).await {
        Ok(_) => {
            // 버퍼 비우기
            server_stream.flush().await?;
            info!("[Session:{}] 서버에 요청 전송 완료", session_id_str);
        },
        Err(e) => {
            error!("[Session:{}] 서버 쓰기 오류: {}", session_id_str, e);
            return Err(e.into());
        }
    }
    
    // 서버로부터 응답 읽기
    let mut total_bytes_out = 0;
    let mut server_buf = BytesMut::with_capacity(BUFFER_SIZE_MEDIUM);
    
    // 응답 수신 시작 시간 기록 (요청 전송 완료 후)
    let response_start_time = Instant::now();
    
    // 응답 수신 제한 시간 설정 (최대 10초)
    let response_timeout = Duration::from_secs(10);
    
    // 응답 읽기 및 전송
    let mut last_read_time = Instant::now();
    let mut is_chunked = false;
    let mut is_content_length = false;
    let mut content_length = 0;
    let mut body_start_pos = 0;
    let mut headers_received = false;
    let mut first_response_received = false;
    let mut first_response_time = None;
    
    // 응답 읽기 루프
    loop {
        // 버퍼 할당
        let mut buf = BytesMut::with_capacity(8192);
        
        // 서버로부터 데이터 읽기 (타임아웃 적용)
        match tokio::time::timeout(
            Duration::from_millis(200),  // 200ms 타임아웃으로 자주 확인
            server_stream.read_buf(&mut buf)
        ).await {
            Ok(Ok(0)) => {
                info!("[Session:{}] 서버 응답 완료 (연결 종료)", session_id_str);
                break; // 서버 측에서 연결 종료
            },
            Ok(Ok(n)) => {
                total_bytes_out += n;
                metrics.add_http_bytes_out(n as u64);
                last_read_time = Instant::now();
                
                // 첫 응답 수신 시간 기록 (아직 기록되지 않은 경우)
                if !first_response_received {
                    first_response_received = true;
                    first_response_time = Some(Instant::now().duration_since(response_start_time).as_millis() as u64);
                    debug!("[Session:{}] 첫 응답 수신 시간: {}ms", session_id_str, first_response_time.unwrap());
                }
                
                // 응답 데이터를 버퍼에 추가
                server_buf.put_slice(&buf[..n]);
                
                // 헤더 분석 (첫 응답 청크에서만 수행)
                if !headers_received && server_buf.len() > 4 {
                    // HTTP 헤더의 끝 찾기
                    if let Some(pos) = find_header_end(&server_buf) {
                        headers_received = true;
                        body_start_pos = pos + 4; // "\r\n\r\n" 건너뛰기
                        
                        // HTTP 응답 상태 코드 확인
                        let header_text = String::from_utf8_lossy(&server_buf[..pos]);
                        let status_line = header_text.lines().next().unwrap_or("");
                        
                        debug!("[Session:{}] HTTP 응답 상태: {}", session_id_str, status_line);
                        
                        // Transfer-Encoding: chunked 확인
                        if header_text.to_lowercase().contains("transfer-encoding: chunked") {
                            is_chunked = true;
                            debug!("[Session:{}] Chunked 응답 감지", session_id_str);
                        }
                        
                        // Content-Length 헤더 확인
                        for line in header_text.lines() {
                            if line.to_lowercase().starts_with("content-length:") {
                                if let Some(len_str) = line.splitn(2, ':').nth(1) {
                                    if let Ok(len) = len_str.trim().parse::<usize>() {
                                        is_content_length = true;
                                        content_length = len;
                                        debug!("[Session:{}] Content-Length: {} 바이트", session_id_str, content_length);
                                        break;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                
                // 클라이언트에 응답 전송
                if let Err(e) = client_stream.write_all(&buf[..n]).await {
                    if e.kind() == io::ErrorKind::BrokenPipe || e.kind() == io::ErrorKind::ConnectionReset {
                        debug!("[Session:{}] 클라이언트 연결 종료됨: {}", session_id_str, e);
                        break;
                    } else {
                        error!("[Session:{}] 클라이언트 쓰기 오류: {}", session_id_str, e);
                        return Err(e.into());
                    }
                }
                
                // 연결 종료 조건 확인
                
                // 1. Content-Length 기반 종료 확인
                if is_content_length && headers_received {
                    let current_body_size = server_buf.len().saturating_sub(body_start_pos);
                    if current_body_size >= content_length {
                        debug!("[Session:{}] 응답 완료 (Content-Length 충족)", session_id_str);
                        break;
                    }
                }
                
                // 2. Chunked 인코딩 종료 확인
                if is_chunked && server_buf.len() > 5 {
                    // 마지막 청크 (0\r\n\r\n) 확인
                    if find_chunk_end(&server_buf) {
                        debug!("[Session:{}] 응답 완료 (최종 청크 수신)", session_id_str);
                        break;
                    }
                }
            },
            Ok(Err(e)) => {
                error!("[Session:{}] 서버 읽기 오류: {}", session_id_str, e);
                break;
            },
            Err(_) => {
                // 타임아웃 - 1초 이상 데이터가 없으면 확인
                if last_read_time.elapsed() > Duration::from_secs(1) {
                    info!("[Session:{}] 응답 데이터 없음 (1초 타임아웃, {}바이트 수신됨)", 
                         session_id_str, total_bytes_out);
                    
                    // 이미 일부 데이터를 받았고, 헤더가 완료된 경우에는 완료로 간주
                    if total_bytes_out > 0 && headers_received {
                            break;
                        }
                }
                
                // 전체 제한 시간 확인
                if response_start_time.elapsed() > response_timeout {
                    warn!("[Session:{}] 응답 시간 초과 ({}초)", session_id_str, response_timeout.as_secs());
                                                    break;
                                                }
                                                
                continue; // 다시 읽기 시도
            }
        }
    }
    
    info!("[Session:{}] 서버 응답 {} 바이트 수신 완료", session_id_str, total_bytes_out);
    
    // 응답 시간 계산
    let response_time;
    
    // 첫 응답 시간을 우선 사용하고, 없으면 전체 응답 시간 사용
    if let Some(first_time) = first_response_time {
        response_time = first_time;
        info!("[Session:{}] HTTP 요청 완료, 첫 응답 시간: {}ms", session_id_str, response_time);
                                                        } else {
        response_time = response_start_time.elapsed().as_millis() as u64;
        info!("[Session:{}] HTTP 요청 완료, 응답 시간: {}ms", session_id_str, response_time);
    }
    
    // 응답 시간 상한 설정 (비정상적으로 긴 응답 시간 방지)
    let capped_response_time = if response_time > 60000 {
        warn!("[Session:{}] 비정상적으로 긴 응답 시간 ({}ms)을 60초로 제한", session_id_str, response_time);
        60000
                                                        } else {
        response_time
    };
    
    // 파비콘 등 자동 요청인지 확인
    let is_auto_request = if let Ok(request_text) = std::str::from_utf8(&client_buf) {
        request_text.contains("GET /favicon.ico") || 
        request_text.contains("GET /robots.txt") || 
        request_text.contains("GET /.well-known")
                        } else {
        false
    };
    
    // 자동 요청이 아닐 때만 응답 시간 DB에 기록
    if !is_auto_request {
        // 응답 시간 업데이트 - 별도 태스크로 실행
        if let Ok(logger) = REQUEST_LOGGER.try_read() {
            // 세션 ID와 응답 시간 복사
            let session_id_copy = session_id_str.clone();
            let response_time_copy = capped_response_time;
            
                                        tokio::spawn(async move {
                // 짧은 응답의 경우 DB 작업 완료를 위해 추가 지연
                if response_time_copy < 500 {
                    debug!("[Session:{}] HTTP 응답이 빠름 ({}ms), DB 업데이트를 위해 300ms 대기", 
                          session_id_copy, response_time_copy);
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
                
                // 응답 시간 업데이트 3회 재시도
                for attempt in 1..=3 {
                    match logger.update_response_time(&session_id_copy, response_time_copy).await {
                        Ok(_) => {
                            info!("[Session:{}] HTTP 응답 시간 업데이트 성공: {}ms (시도 {}/3)", 
                                 session_id_copy, response_time_copy, attempt);
                            break;
                        },
                        Err(e) => {
                            if attempt < 3 {
                                warn!("[Session:{}] HTTP 응답 시간 업데이트 실패 (시도 {}/3): {}, 재시도 중...", 
                                     session_id_copy, attempt, e);
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            } else {
                                error!("[Session:{}] HTTP 응답 시간 업데이트 최종 실패: {}", 
                                      session_id_copy, e);
                            }
                        }
                    }
                }
            });
        } else {
            warn!("[Session:{}] RequestLogger 인스턴스에 접근할 수 없음", session_id_str);
        }
    } else {
        debug!("[Session:{}] 브라우저 자동 요청 감지됨 - 응답 시간 업데이트 생략", session_id_str);
    }
    
    // 연결 종료 전 확인
    info!("[Session:{}] HTTP 프록시 완료 - 수신: {} 바이트, 전송: {} 바이트", 
          session_id_str, total_bytes_in, total_bytes_out);
    
    // 클라이언트 연결 종료 시도
    if let Err(e) = client_stream.shutdown().await {
        debug!("[Session:{}] 클라이언트 연결 종료 실패: {}", session_id_str, e);
    }
    
    // 서버 연결 종료 시도
    if let Err(e) = server_stream.shutdown().await {
        debug!("[Session:{}] 서버 연결 종료 실패: {}", session_id_str, e);
    }
    
    Ok(())
} 