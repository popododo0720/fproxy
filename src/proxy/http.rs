use std::sync::Arc;
use std::time::{Instant, Duration};

use log::{debug, error, warn};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::{BytesMut, BufMut};
use base64::{self, engine::general_purpose::STANDARD, Engine};

use crate::metrics::Metrics;
use crate::config::Config;
use crate::logging::{Logger, LogFormatter};
use crate::error::{ProxyError, Result, http_err, internal_err};

// 기본 상수 정의 (config에서 값을 가져오지 못할 경우 사용)
const DEFAULT_BUFFER_SIZE: usize = 8192;
const DEFAULT_PROXY_TIMEOUT_MS: u64 = 30000; // 30초

/// HTTP 요청 헤더 끝 위치 찾기
fn find_header_end(buffer: &[u8]) -> Option<usize> {
    // \r\n\r\n 패턴 찾기
    for i in 0..buffer.len().saturating_sub(3) {
        if buffer[i] == b'\r' && buffer[i+1] == b'\n' && buffer[i+2] == b'\r' && buffer[i+3] == b'\n' {
            return Some(i);
        }
    }
    None
}

/// 헤더에서 Content-Length 값 추출
fn extract_content_length(headers: &str) -> Option<usize> {
    for line in headers.lines() {
        if line.to_lowercase().starts_with("content-length:") {
            if let Some(length_str) = line.split(':').nth(1) {
                if let Ok(length) = length_str.trim().parse::<usize>() {
                    return Some(length);
                }
            }
        }
    }
    None
}

/// HTTP 응답 상태 코드 추출
fn extract_status_code(headers: &str) -> Option<u16> {
    let first_line = headers.lines().next()?;
    let parts: Vec<&str> = first_line.split_whitespace().collect();
    if parts.len() >= 2 {
        parts[1].parse::<u16>().ok()
    } else {
        None
    }
}

/// HTTP 요청에서 기본 정보 추출 (메서드, 경로, 호스트, 헤더 끝 위치)
fn parse_http_request_basic(request: &str) -> (String, String, Option<String>, Option<usize>) {
    let mut method = String::from("UNKNOWN");
    let mut path = String::from("/");
    let mut host = None;
    let mut header_end = None;
    
    // 요청 라인 파싱
    if let Some(first_line) = request.lines().next() {
        let parts: Vec<&str> = first_line.split_whitespace().collect();
        if parts.len() >= 2 {
            method = parts[0].to_string();
            path = parts[1].to_string();
        }
    }
    
    // 호스트 헤더 찾기
    for line in request.lines() {
        if line.is_empty() {
            break;
        }
        
        if line.to_lowercase().starts_with("host:") {
            if let Some(h) = line.splitn(2, ':').nth(1) {
                host = Some(h.trim().to_string());
            }
        }
    }
    
    // 헤더 끝 위치 찾기
    if let Some(pos) = request.find("\r\n\r\n") {
        header_end = Some(pos);
    }
    
    (method, path, host, header_end)
}

/// 간소화된 HTTP 프록시 함수
pub async fn proxy_http_streams(
    mut client_stream: TcpStream,
    mut server_stream: TcpStream,
    metrics: Arc<Metrics>,
    session_id: &str,
    request_start_time: Instant,
    config: Option<Arc<Config>>,
    initial_request: Option<Vec<u8>>,
    already_logged: bool,
    logger: Option<Arc<Logger>>,
) -> Result<()> {
    // 세션 ID를 문자열로 복제하여 일관된 사용 보장
    let session_id_str = session_id.to_string();
    
    // config에서 buffer_size 가져오기
    let buffer_size = config.as_ref()
        .map(|c| c.buffer_size)
        .unwrap_or(DEFAULT_BUFFER_SIZE);
    
    // 버퍼 초기화
    let mut client_buf = BytesMut::with_capacity(buffer_size);
    let mut server_buf = BytesMut::with_capacity(buffer_size);
    
    // 초기 요청이 있으면 서버로 전송
    if let Some(initial_data) = initial_request {
        // 초기 요청 데이터 복사
        client_buf.put_slice(&initial_data);
        
        // 요청 로깅 (아직 로깅되지 않은 경우)
        if !already_logged {
            // 요청 텍스트 변환
            let request_text = String::from_utf8_lossy(&client_buf).to_string();
            
            // 기본 요청 정보 파싱
            let (method, path, host_opt, _) = parse_http_request_basic(&request_text);
            let host = host_opt.unwrap_or_else(|| "unknown.host".to_string());
            
            // 로거가 있으면 요청 로깅
            if let Some(logger) = &logger {
                if let Err(e) = logger.log_request(
                    host.clone(),
                    method,
                    path,
                    request_text,
                    None, // body
                    session_id_str.clone(),
                    "0.0.0.0".to_string(), // client_ip
                    "0.0.0.0".to_string(), // target_ip
                    false, // is_rejected
                    false, // is_tls
                ).await {
                    error!("[Session:{}] 요청 로깅 실패: {}", session_id_str, e);
                }
            }
        }
        
        // 서버로 전송
        if let Err(e) = server_stream.write_all(&client_buf).await {
            error!("[Session:{}] 서버에 초기 요청 전송 실패: {}", session_id_str, e);
            return Err(ProxyError::from(e));
        }
        
        // 버퍼 비우기
        client_buf.clear();
    }
    
    // config에서 timeout_ms 가져오기
    let timeout_ms = config.as_ref()
        .map(|c| c.timeout_ms as u64)
        .unwrap_or(DEFAULT_PROXY_TIMEOUT_MS);
    
    // 타임아웃 설정
    let timeout_duration = Duration::from_millis(timeout_ms);
    
    // 완료 플래그
    let mut _is_complete = false;
    
    // 헤더 끝 위치
    let mut header_end_pos = None;
    
    // 응답 완료 감지를 위한 변수
    let mut content_length: Option<usize> = None;
    let mut is_chunked = false;
    let mut received_bytes = 0;
    let mut chunked_complete = false;
    
    // 서버로부터 응답 읽기
    loop {
        // 타임아웃 설정
        match tokio::time::timeout(timeout_duration, server_stream.read_buf(&mut server_buf)).await {
            Ok(result) => {
                match result {
                    Ok(0) => {
                        // 서버가 연결을 닫음
                        debug!("[Session:{}] 서버가 연결을 닫음", session_id_str);
                        break;
                    }
                    Ok(n) => {
                        debug!("[Session:{}] 서버로부터 {}바이트 수신", session_id_str, n);
                        
                        // 헤더 끝 위치를 아직 찾지 못했다면 찾기
                        if header_end_pos.is_none() {
                            if let Some(pos) = find_header_end(&server_buf) {
                                header_end_pos = Some(pos);
                                
                                // 헤더를 찾았으면 Content-Length 또는 Transfer-Encoding: chunked 확인
                                let headers = String::from_utf8_lossy(&server_buf[..pos]).to_string();
                                
                                // Content-Length 확인
                                if let Some(length) = extract_content_length(&headers) {
                                    content_length = Some(length);
                                    debug!("[Session:{}] Content-Length: {}", session_id_str, length);
                                }
                                
                                // Transfer-Encoding: chunked 확인
                                if headers.to_lowercase().contains("transfer-encoding: chunked") {
                                    is_chunked = true;
                                    debug!("[Session:{}] Chunked encoding detected", session_id_str);
                                }
                            }
                        }
                        
                        // 클라이언트에 전송
                        if let Err(e) = client_stream.write_all(&server_buf[server_buf.len() - n..]).await {
                            error!("[Session:{}] 클라이언트에 응답 전송 실패: {}", session_id_str, e);
                            return Err(ProxyError::Http(format!("클라이언트에 응답 전송 실패: {}", e)));
                        }
                        
                        // 응답 완료 감지 로직
                        if let Some(pos) = header_end_pos {
                            if let Some(length) = content_length {
                                // Content-Length 기반 완료 감지
                                received_bytes += n;
                                let body_received = server_buf.len() - pos - 4; // 헤더 끝 패턴(\r\n\r\n) 길이 4 빼기
                                
                                if body_received >= length {
                                    debug!("[Session:{}] 응답 완료 감지: Content-Length 기반 ({}바이트)", session_id_str, body_received);
                                    _is_complete = true;
                                    break;
                                }
                            } else if is_chunked {
                                // Chunked 인코딩 기반 완료 감지
                                // 마지막 청크(0 크기 청크)를 찾음
                                let buf_str = String::from_utf8_lossy(&server_buf).to_string();
                                if buf_str.contains("\r\n0\r\n\r\n") {
                                    debug!("[Session:{}] 응답 완료 감지: Chunked 인코딩 종료 마커 발견", session_id_str);
                                    chunked_complete = true;
                                    _is_complete = true;
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("[Session:{}] 서버로부터 읽기 오류: {}", session_id_str, e);
                        return Err(ProxyError::Http(format!("서버로부터 읽기 오류: {}", e)));
                    }
                }
            }
            Err(_) => {
                warn!("[Session:{}] 서버 읽기 타임아웃, 정상 종료로 처리", session_id_str);
                _is_complete = true;
                break;
            }
        }
    }
    
    // 응답 시간 계산
    let response_time = request_start_time.elapsed().as_millis() as u64;
    
    // 응답 로깅
    if let Some(logger) = &logger {
        // 헤더 추출
        let headers = if let Some(pos) = header_end_pos {
            if let Ok(headers_text) = std::str::from_utf8(&server_buf[..pos]) {
                headers_text.to_string()
            } else {
                String::from_utf8_lossy(&server_buf[..pos]).into_owned()
            }
        } else {
            // 헤더 끝을 찾지 못한 경우 전체 버퍼를 사용
            String::from_utf8_lossy(&server_buf).into_owned()
        };
        
        // 응답 본문 미리보기 (최대 1KB)
        let body_preview = if let Some(pos) = header_end_pos {
            if server_buf.len() > pos + 4 {
                let preview_end = std::cmp::min(pos + 4 + 1024, server_buf.len());
                let body_slice = &server_buf[pos + 4..preview_end];
                
                if let Ok(body_text) = std::str::from_utf8(body_slice) {
                    Some(LogFormatter::summarize_body(body_text, 1024))
                } else {
                    // 바이너리 데이터를 base64로 인코딩
                    let base64_data = STANDARD.encode(body_slice);
                    Some(LogFormatter::summarize_body(&base64_data, 1024))
                }
            } else {
                None
            }
        } else {
            None
        };
        
        // 응답 로그 저장
        if let Err(e) = logger.log_response(
            session_id_str.clone(),
            extract_status_code(&headers).unwrap_or(0),
            response_time,
            server_buf.len(), // response_size
            headers,
            body_preview,
        ).await {
            error!("[Session:{}] 응답 로깅 실패: {}", session_id_str, e);
        }
    }
    
    // 세션 완료 로깅
    debug!("[Session:{}] HTTP 세션 완료: {} ms", session_id_str, response_time);
    
    Ok(())
}

/// 클라이언트와 서버 간 양방향 HTTP 프록시
pub async fn bidirectional_http_proxy(
    mut client_stream: TcpStream,
    mut server_stream: TcpStream,
    _metrics: Arc<Metrics>,
    session_id: &str,
    config: Option<Arc<Config>>,
    logger: Option<Arc<Logger>>,
) -> Result<()> {
    // 세션 ID를 문자열로 복제하여 일관된 사용 보장
    let session_id_str = session_id.to_string();
    
    // config에서 buffer_size 가져오기
    let buffer_size = config.as_ref()
        .map(|c| c.buffer_size)
        .unwrap_or(DEFAULT_BUFFER_SIZE);
    
    // 버퍼 초기화
    let mut client_buf = BytesMut::with_capacity(buffer_size);
    
    // 요청 시작 시간
    let _request_start_time = Instant::now();
    
    // config에서 timeout_ms 가져오기
    let timeout_ms = config.as_ref()
        .map(|c| c.timeout_ms as u64)
        .unwrap_or(DEFAULT_PROXY_TIMEOUT_MS);
    
    // 타임아웃 설정
    let timeout_duration = Duration::from_millis(timeout_ms);
    
    // 완료 플래그
    let mut _is_complete = false;
    
    // 클라이언트로부터 초기 요청 읽기
    match tokio::time::timeout(timeout_duration, client_stream.read_buf(&mut client_buf)).await {
        Ok(result) => {
            match result {
                Ok(0) => {
                    debug!("[Session:{}] 클라이언트가 연결을 닫음", session_id_str);
                    return Ok(());
                }
                Ok(n) => {
                    debug!("[Session:{}] 클라이언트로부터 {}바이트 수신", session_id_str, n);
                    
                    // 요청 텍스트 변환
                    let request_text = String::from_utf8_lossy(&client_buf).to_string();
                    
                    // 기본 요청 정보 파싱
                    let (method, path, host_opt, _) = parse_http_request_basic(&request_text);
                    let host = host_opt.unwrap_or_else(|| "unknown.host".to_string());
                    
                    // 로거가 있으면 요청 로깅
                    if let Some(logger) = &logger {
                        if let Err(e) = logger.log_request(
                            host.clone(),
                            method,
                            path,
                            request_text,
                            None, // body
                            session_id_str.clone(),
                            "0.0.0.0".to_string(), // client_ip
                            "0.0.0.0".to_string(), // target_ip
                            false, // is_rejected
                            false, // is_tls
                        ).await {
                            error!("[Session:{}] 요청 로깅 실패: {}", session_id_str, e);
                        }
                    }
                    
                    // 서버로 전송
                    if let Err(e) = server_stream.write_all(&client_buf).await {
                        error!("[Session:{}] 서버에 요청 전송 실패: {}", session_id_str, e);
                        return Err(ProxyError::Http(format!("서버로부터 읽기 오류: {}", e)));
                    }
                }
                Err(e) => {
                    error!("[Session:{}] 클라이언트로부터 읽기 오류: {}", session_id_str, e);
                    return Err(ProxyError::Http(format!("클라이언트로부터 읽기 오류: {}", e)));
                }
            }
        }
        Err(_) => {
            warn!("[Session:{}] 클라이언트 읽기 타임아웃", session_id_str);
            return Ok(());
        }
    }
    
    // 양방향 프록시 시작
    let (mut client_read, mut client_write) = tokio::io::split(client_stream);
    let (mut server_read, mut server_write) = tokio::io::split(server_stream);
    
    // 클라이언트 -> 서버 방향 프록시
    let client_to_server = tokio::spawn(async move {
        let mut buffer = BytesMut::with_capacity(buffer_size);
        loop {
            match client_read.read_buf(&mut buffer).await {
                Ok(0) => break,
                Ok(n) => {
                    if let Err(e) = server_write.write_all(&buffer[buffer.len() - n..]).await {
                        error!("서버에 데이터 전송 실패: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    error!("클라이언트로부터 읽기 오류: {}", e);
                    break;
                }
            }
        }
    });
    
    // 서버 -> 클라이언트 방향 프록시
    let server_to_client = tokio::spawn(async move {
        let mut buffer = BytesMut::with_capacity(buffer_size);
        loop {
            match server_read.read_buf(&mut buffer).await {
                Ok(0) => break,
                Ok(n) => {
                    if let Err(e) = client_write.write_all(&buffer[buffer.len() - n..]).await {
                        error!("클라이언트에 데이터 전송 실패: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    error!("서버로부터 읽기 오류: {}", e);
                    break;
                }
            }
        }
    });
    
    // 양방향 프록시 완료 대기
    let _ = tokio::join!(client_to_server, server_to_client);
    
    // 세션 완료 로깅
    debug!("[Session:{}] 양방향 HTTP 세션 완료", session_id_str);
    
    Ok(())
}
