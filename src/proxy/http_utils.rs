use std::sync::Arc;
use std::time::Duration;

use bytes::{BytesMut, Bytes};
use log::{debug, error, warn};
use tokio::net::TcpStream;
use base64::engine::general_purpose::STANDARD;
use base64::prelude::*;
use tokio::io::AsyncWriteExt;

use crate::config::Config;
use crate::constants::{DEFAULT_BUFFER_SIZE, DEFAULT_PROXY_TIMEOUT_MS};
use crate::error::{ProxyError, Result};
use crate::logging::{Logger, LogFormatter};

/// HTTP 요청 기본 정보
pub struct HttpRequestInfo {
    pub method: String,
    pub path: String,
    pub host: String,
    pub content_length: Option<usize>,
}

/// 헤더 끝 위치 찾기
pub fn find_header_end(buffer: &[u8]) -> Option<usize> {
    for i in 0..buffer.len().saturating_sub(3) {
        if buffer[i] == b'\r' && buffer[i + 1] == b'\n' && buffer[i + 2] == b'\r' && buffer[i + 3] == b'\n' {
            return Some(i);
        }
    }
    None
}

/// Content-Length 헤더 추출 - 개선된 버전
pub fn extract_content_length(headers: &str) -> Option<usize> {
    // 대소문자 구분 없이 처리하기 위해 로우케이스로 변환하지 않고 대소문자 무관한 검색 사용
    for line in headers.lines() {
        let lowercase_line = line.to_lowercase();
        if lowercase_line.starts_with("content-length:") {
            if let Some(len_str) = line.split(':').nth(1) {
                if let Ok(len) = len_str.trim().parse::<usize>() {
                    debug!("Content-Length 값 추출: {}", len);
                    return Some(len);
                } else {
                    warn!("Content-Length 값 파싱 실패: {}", len_str.trim());
                }
            }
        }
    }
    None
}

/// 응답에서 청크 인코딩 사용 여부 확인
pub fn is_chunked_encoding(headers: &str) -> bool {
    for line in headers.lines() {
        let lowercase_line = line.to_lowercase();
        if lowercase_line.starts_with("transfer-encoding:") && lowercase_line.contains("chunked") {
            return true;
        }
    }
    false
}

/// 상태 코드 추출
pub fn extract_status_code(headers: &str) -> Option<u16> {
    if let Some(first_line) = headers.lines().next() {
        let parts: Vec<&str> = first_line.split_whitespace().collect();
        if parts.len() >= 2 && parts[0].starts_with("HTTP/") {
            if let Ok(code) = parts[1].parse::<u16>() {
                return Some(code);
            }
        }
    }
    None
}

/// HTTP 요청 기본 정보 파싱
pub fn parse_http_request_basic(request: &str) -> HttpRequestInfo {
    let mut method = String::from("UNKNOWN");
    let mut path = String::from("/");
    let mut host = String::from("unknown.host");
    let mut content_length = None;
    
    // 첫 줄 파싱
    if let Some(first_line) = request.lines().next() {
        let parts: Vec<&str> = first_line.split_whitespace().collect();
        if parts.len() >= 2 {
            method = parts[0].to_string();
            path = parts[1].to_string();
        }
    }
    
    // 헤더 파싱
    for line in request.lines() {
        let lower_line = line.to_lowercase();
        
        // Host 헤더 찾기
        if lower_line.starts_with("host:") {
            if let Some(host_value) = line.split(':').nth(1) {
                host = host_value.trim().to_string();
            }
        }
        
        // Content-Length 헤더 찾기
        if lower_line.starts_with("content-length:") {
            if let Some(len_str) = line.split(':').nth(1) {
                if let Ok(len) = len_str.trim().parse::<usize>() {
                    content_length = Some(len);
                }
            }
        }
    }
    
    HttpRequestInfo {
        method,
        path,
        host,
        content_length,
    }
}



/// 클라이언트 요청을 서버로 전송 - 최적화 버전
pub async fn send_client_request_to_server(
    client_buf: &BytesMut,
    server_stream: &mut TcpStream,
    session_id: &str,
) -> Result<()> {
    // 요청 내용 디버그 로깅 (첫 100바이트만)
    let preview_size = std::cmp::min(100, client_buf.len());
    let request_preview = String::from_utf8_lossy(&client_buf[0..preview_size]);
    debug!("[Session:{}] 서버로 전송할 요청 미리보기: {}", session_id, request_preview);
    
    // 데이터를 한 번에 쓰기 시도
    match server_stream.write_all(client_buf).await {
        Ok(_) => {
            // 즉시 플러시하여 데이터가 전송되도록 보장
            match server_stream.flush().await {
                Ok(_) => {
                    debug!("[Session:{}] 서버에 {}바이트 요청 전송 완료", session_id, client_buf.len());
                    Ok(())
                },
                Err(e) => {
                    // 연결 종료 관련 오류는 정상 처리로 간주
                    if e.kind() == std::io::ErrorKind::ConnectionReset || 
                       e.kind() == std::io::ErrorKind::ConnectionAborted || 
                       e.kind() == std::io::ErrorKind::BrokenPipe {
                        debug!("[Session:{}] 서버 연결 정상 종료 (플러시 중): {}", session_id, e);
                        return Ok(());
                    }
                    error!("[Session:{}] 서버에 요청 플러시 실패: {}", session_id, e);
                    Err(ProxyError::Http(format!("서버에 요청 플러시 실패: {}", e)))
                }
            }
        },
        Err(e) => {
            // 연결 종료 관련 오류는 정상 처리로 간주
            if e.kind() == std::io::ErrorKind::ConnectionReset || 
               e.kind() == std::io::ErrorKind::ConnectionAborted || 
               e.kind() == std::io::ErrorKind::BrokenPipe {
                debug!("[Session:{}] 서버 연결 정상 종료 (쓰기 중): {}", session_id, e);
                return Ok(());
            }
            error!("[Session:{}] 서버에 요청 전송 실패: {}", session_id, e);
            Err(ProxyError::Http(format!("서버에 요청 전송 실패: {}", e)))
        }
    }
}

/// 서버 응답을 클라이언트로 전송 - 최적화 버전
pub async fn send_server_response_to_client(
    server_buf: &BytesMut,
    bytes_received: usize,
    client_stream: &mut TcpStream,
    session_id: &str,
) -> Result<()> {
    // 전송할 데이터 조각 가져오기
    let data_to_send = &server_buf[server_buf.len() - bytes_received..];
    
    // 데이터를 한 번에 쓰기 시도
    match client_stream.write_all(data_to_send).await {
        Ok(_) => {
            // 즉시 플러시하여 데이터가 전송되도록 보장
            match client_stream.flush().await {
                Ok(_) => {
                    debug!("[Session:{}] 클라이언트에 {}바이트 응답 전송 완료", session_id, bytes_received);
                    Ok(())
                },
                Err(e) => {
                    error!("[Session:{}] 클라이언트에 응답 플러시 실패: {}", session_id, e);
                    Err(ProxyError::Http(format!("클라이언트에 응답 플러시 실패: {}", e)))
                }
            }
        },
        Err(e) => {
            error!("[Session:{}] 클라이언트에 응답 전송 실패: {}", session_id, e);
            Err(ProxyError::Http(format!("클라이언트에 응답 전송 실패: {}", e)))
        }
    }
}

/// 응답 완료 여부 확인 - 개선된 버전
pub fn is_response_complete(
    server_buf: &BytesMut,
    header_end_pos: usize,
    session_id: &str,
) -> bool {
    // 헤더 영역이 없는 경우 완료로 간주
    if header_end_pos == 0 || header_end_pos >= server_buf.len() {
        debug!("[Session:{}] 헤더 영역이 없거나 버퍼보다 크기 때문에 완료로 간주", session_id);
        return true;
    }

    // 헤더 끝 패턴(\r\n\r\n) 길이
    let header_end_pattern_len = 4;
    if header_end_pos + header_end_pattern_len > server_buf.len() {
        debug!("[Session:{}] 헤더 영역이 버퍼보다 크기 때문에 미완료로 간주", session_id);
        return false;
    }

    // 헤더에서 응답 정보 추출
    let headers = &server_buf[..header_end_pos];
    let headers_str = String::from_utf8_lossy(headers);
    
    // 상태 코드 추출
    let status_code = extract_status_code(&headers_str).unwrap_or(0);
    
    // 1xx, 204, 304 응답은 본문이 없음
    if (status_code >= 100 && status_code < 200) || status_code == 204 || status_code == 304 {
        debug!("[Session:{}] 상태 코드 {}(본문 없음)으로 완료로 간주", session_id, status_code);
        return true;
    }
    
    // Content-Length 기반 완료 감지
    if let Some(length) = extract_content_length(&headers_str) {
        let body_received = server_buf.len() - header_end_pos - header_end_pattern_len;
        
        if body_received >= length {
            debug!("[Session:{}] Content-Length({}) 기반 완료 감지: 받은 본문 {} 바이트", session_id, length, body_received);
            return true;
        } else {
            debug!("[Session:{}] Content-Length({}) 기반 미완료: 받은 본문 {} 바이트", session_id, length, body_received);
            return false;
        }
    } 
    
    // Chunked 인코딩 기반 완료 감지
    if is_chunked_encoding(&headers_str) {
        let body = &server_buf[header_end_pos + header_end_pattern_len..];
        
        // 마지막 청크(0 크기 청크) 패턴: "\r\n0\r\n\r\n"
        if body.len() < 7 {
            debug!("[Session:{}] Chunked 인코딩 미완료: 본문이 너무 짧음 ({} 바이트)", session_id, body.len());
            return false;
        }
        
        // 마지막 청크 찾기
        for i in 0..body.len().saturating_sub(6) {
            if body[i] == b'\r' && body[i+1] == b'\n' && 
               body[i+2] == b'0' && body[i+3] == b'\r' && 
               body[i+4] == b'\n' && body[i+5] == b'\r' && body[i+6] == b'\n' {
                debug!("[Session:{}] Chunked 인코딩 완료 감지: 마지막 청크 발견", session_id);
                return true;
            }
        }
        
        debug!("[Session:{}] Chunked 인코딩 미완료: 마지막 청크 발견 실패", session_id);
        return false;
    } 
    
    // HEAD 요청이나 특정 상태 코드인 경우 본문이 없을 수 있음
    // Content-Length나 Chunked 없이 서버가 연결을 닫은 경우
    debug!("[Session:{}] Content-Length 나 Chunked 없음, 서버 연결 상태에 따라 완료 간주", session_id);
    true
}

/// 응답 로깅 처리
pub async fn log_http_response(
    logger: &Arc<Logger>,
    session_id: &str,
    server_buf: &BytesMut,
    header_end_pos: usize,
    response_time: u64,
) -> Result<()> {
    // 로깅을 위한 데이터를 클론하여 비동기 작업으로 처리
    // BytesMut를 Bytes로 변환하여 참조 카운팅 활용
    let buf_bytes = server_buf.clone().freeze(); // freeze()는 BytesMut를 Bytes로 변환
    let session_id_owned = session_id.to_string();
    let logger_clone = Arc::clone(logger);
    
    // 비동기 작업으로 로깅 처리 - 메인 스트림 처리와 분리
    tokio::spawn(async move {
        // 헤더 추출 - 참조만 사용하여 복사 최소화
        let headers = if let Ok(headers_text) = std::str::from_utf8(&buf_bytes[..header_end_pos]) {
            headers_text.to_string()
        } else {
            String::from_utf8_lossy(&buf_bytes[..header_end_pos]).into_owned()
        };
        
        // 응답 본문 미리보기 (최대 1KB) - 참조만 사용하여 복사 최소화
        let body_preview = if buf_bytes.len() > header_end_pos + 4 {
            let preview_end = std::cmp::min(header_end_pos + 4 + 1024, buf_bytes.len());
            let body_slice = &buf_bytes[header_end_pos + 4..preview_end];
            
            if let Ok(body_text) = std::str::from_utf8(body_slice) {
                Some(LogFormatter::summarize_body(body_text, 1024))
            } else {
                // 바이너리 데이터를 base64로 인코딩
                let base64_data = STANDARD.encode(body_slice);
                Some(LogFormatter::summarize_body(&base64_data, 1024))
            }
        } else {
            None
        };
        
        // 응답 로그 저장
        if let Err(e) = logger_clone.log_response(
            session_id_owned.clone(),
            extract_status_code(&headers).unwrap_or(0),
            response_time,
            buf_bytes.len(), // response_size
            headers,
            body_preview,
        ).await {
            error!("[Session:{}] 응답 로깅 실패: {}", session_id_owned, e);
        }
    });
    
    // 메인 스트림은 즉시 반환하여 처리 지연 없음
    Ok(())
}

/// 요청 로깅 처리 - 최적화 버전
pub async fn log_http_request(
    logger: &Arc<Logger>,
    session_id: &str,
    client_buf: &BytesMut,
    header_end_pos: usize,
    request_info: &HttpRequestInfo,
) -> Result<()> {
    // 헤더 추출
    let headers = String::from_utf8_lossy(&client_buf[..header_end_pos]).to_string();
    
    // 본문 미리보기 추출 (1KB 이하)
    let body_preview = if client_buf.len() > header_end_pos + 4 {
        let preview_end = std::cmp::min(header_end_pos + 4 + 1024, client_buf.len());
        let body_slice = &client_buf[header_end_pos + 4..preview_end];
        
        if let Ok(body_text) = std::str::from_utf8(body_slice) {
            Some(LogFormatter::summarize_body(body_text, 1024))
        } else {
            // 바이너리 데이터를 base64로 인코딩
            let base64_data = STANDARD.encode(body_slice);
            Some(LogFormatter::summarize_body(&base64_data, 1024))
        }
    } else {
        None
    };
    
    // 클라이언트/서버 IP 주소 추출
    let client_ip = "unknown".to_string(); // 필요할 경우 연결에서 추출
    let target_ip = "unknown".to_string(); // 필요할 경우 연결에서 추출
    
    // 비동기적으로 로깅 수행
    if let Err(e) = logger.log_request(
        request_info.host.clone(),
        request_info.method.clone(),
        request_info.path.clone(),
        headers,
        body_preview,
        session_id.to_string(),
        client_ip,
        target_ip,
        false, // is_rejected
        false, // is_tls
    ).await {
        error!("[Session:{}] 요청 로깅 실패: {}", session_id, e);
        return Err(ProxyError::Logging(format!("요청 로깅 실패: {}", e)));
    }
    
    Ok(())
}

/// 설정에서 버퍼 크기 가져오기
pub fn get_buffer_size(config: &Option<Arc<Config>>) -> usize {
    config.as_ref()
        .map(|c| c.buffer_size)
        .unwrap_or(DEFAULT_BUFFER_SIZE)
}

/// 설정에서 타임아웃 가져오기
pub fn get_timeout_duration(config: &Option<Arc<Config>>) -> Duration {
    let timeout_ms = config.as_ref()
        .map(|c| c.timeout_ms as u64)
        .unwrap_or(DEFAULT_PROXY_TIMEOUT_MS);
    
    Duration::from_millis(timeout_ms)
}
