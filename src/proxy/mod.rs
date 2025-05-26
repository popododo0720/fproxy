use std::error::Error;
use std::sync::Arc;
use std::io;

use log::{debug, error, info};
use tokio::net::TcpStream;
use tokio_rustls::{server::TlsStream as ServerTlsStream, client::TlsStream as ClientTlsStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::fs::File;

use crate::metrics::Metrics;
use crate::constants::*;

// 버퍼 크기
// const BUFFER_SIZE_SMALL: usize = 64 * 1024; // 64KB

/// HTTP 스트림 간에 데이터를 전달하고 검사합니다
pub async fn proxy_http_streams(
    client_stream: TcpStream,
    server_stream: TcpStream,
    metrics: Arc<Metrics>,
    session_id: &str
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (mut client_read, mut client_write) = tokio::io::split(client_stream);
    let (mut server_read, mut server_write) = tokio::io::split(server_stream);
    
    // 양방향 데이터 전송 설정
    let client_to_server = async {
        let mut buffer = vec![0u8; BUFFER_SIZE_SMALL];
        let mut total_bytes = 0u64;
        
        loop {
            match client_read.read(&mut buffer).await {
                Ok(0) => break, // 연결 종료
                Ok(n) => {
                    // 서버로 데이터 전송
                    if let Err(e) = server_write.write_all(&buffer[0..n]).await {
                        error!("[Session:{}] Failed to write to server: {}", session_id, e);
                        break;
                    }
                    
                    // 메트릭스 업데이트 (실시간)
                    total_bytes += n as u64;
                    metrics.add_http_bytes_in(n as u64);
                    
                    // 주기적으로 로그 출력 (1MB 마다)
                    if total_bytes % (1024 * 1024) < (n as u64) {
                        debug!("[Session:{}] HTTP 클라이언트→서버 누적: {} KB", 
                               session_id, total_bytes / 1024);
                    }
                },
                Err(e) => {
                    error!("[Session:{}] Error reading from client: {}", session_id, e);
                    break;
                }
            }
        }
        
        debug!("[Session:{}] Client to server transfer finished: {} bytes", session_id, total_bytes);
        Ok::<u64, io::Error>(total_bytes)
    };
    
    let server_to_client = async {
        let mut buffer = vec![0u8; BUFFER_SIZE_SMALL];
        let mut total_bytes = 0u64;
        
        loop {
            match server_read.read(&mut buffer).await {
                Ok(0) => break, // 연결 종료
                Ok(n) => {
                    // 클라이언트로 데이터 전송
                    if let Err(e) = client_write.write_all(&buffer[0..n]).await {
                        error!("[Session:{}] Failed to write to client: {}", session_id, e);
                        break;
                    }
                    
                    // 메트릭스 업데이트 (실시간)
                    total_bytes += n as u64;
                    metrics.add_http_bytes_out(n as u64);
                    
                    // 주기적으로 로그 출력 (1MB 마다)
                    if total_bytes % (1024 * 1024) < (n as u64) {
                        debug!("[Session:{}] HTTP 서버→클라이언트 누적: {} KB", 
                               session_id, total_bytes / 1024);
                    }
                },
                Err(e) => {
                    error!("[Session:{}] Error reading from server: {}", session_id, e);
                    break;
                }
            }
        }
        
        debug!("[Session:{}] Server to client transfer finished: {} bytes", session_id, total_bytes);
        Ok::<u64, io::Error>(total_bytes)
    };
    
    // 양방향 전송 동시 실행
    tokio::select! {
        res = client_to_server => {
            if let Err(e) = res {
                error!("[Session:{}] Client to server transfer failed: {}", session_id, e);
            }
        },
        res = server_to_client => {
            if let Err(e) = res {
                error!("[Session:{}] Server to client transfer failed: {}", session_id, e);
            }
        },
    }
    
    info!("[Session:{}] HTTP proxy completed", session_id);
    Ok(())
}

/// TLS 스트림 간에 데이터를 전달하고 검사합니다
pub async fn proxy_tls_streams(
    client_stream: ServerTlsStream<TcpStream>,
    server_stream: ClientTlsStream<TcpStream>,
    metrics: Arc<Metrics>,
    session_id: &str,
    host: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // 양방향 데이터 전송 및 검사 로직 구현
    let (mut client_read, mut client_write) = tokio::io::split(client_stream);
    let (mut server_read, mut server_write) = tokio::io::split(server_stream);
    
    // 바이트 카운터 초기화
    let client_to_server = async {
        let mut buffer = vec![0u8; BUFFER_SIZE_MEDIUM];
        let mut total_bytes = 0u64;
        
        // 요청 저장을 위한 파일 생성
        let request_log_path = format!("logs/request_{}.log", host);
        let mut request_log_file = match tokio::fs::File::create(&request_log_path).await {
            Ok(file) => Some(file),
            Err(e) => {
                error!("[Session:{}] Failed to create request log file: {}", session_id, e);
                None
            }
        };
        
        // 누적 버퍼 (HTTP 요청이 여러 패킷으로 나뉘어 올 경우를 대비)
        let mut accumulated_data = Vec::new();
        let mut processing_request = false;
        
        loop {
            match client_read.read(&mut buffer).await {
                Ok(0) => break, // 연결 종료
                Ok(n) => {
                    // 서버로 데이터 전송
                    if let Err(e) = server_write.write_all(&buffer[0..n]).await {
                        error!("[Session:{}] Failed to write to server: {}", session_id, e);
                        break;
                    }
                    
                    // 메트릭스 업데이트 (실시간)
                    total_bytes += n as u64;
                    metrics.add_tls_bytes_in(n as u64);
                    
                    // 복호화된 데이터 처리 및 중간 수준 로깅
                    if let Some(file) = &mut request_log_file {
                        // 데이터를 누적 버퍼에 추가
                        accumulated_data.extend_from_slice(&buffer[0..n]);
                        
                        // 데이터를 문자열로 변환 시도
                        if let Ok(data_str) = std::str::from_utf8(&accumulated_data) {
                            // HTTP 요청 시작 감지
                            if !processing_request && 
                               (data_str.starts_with("GET ") || data_str.starts_with("POST ") || 
                                data_str.starts_with("PUT ") || data_str.starts_with("DELETE ") || 
                                data_str.starts_with("HEAD ") || data_str.starts_with("OPTIONS ")) {
                                processing_request = true;
                            }
                            
                            // HTTP 요청 처리
                            if processing_request {
                                // 헤더와 본문 분리 시도
                                if data_str.contains("\r\n\r\n") {
                                    let parts: Vec<&str> = data_str.split("\r\n\r\n").collect();
                                    let headers = parts[0];
                                    
                                    // 헤더 라인 분리
                                    let header_lines: Vec<&str> = headers.lines().collect();
                                    
                                    // 요청 라인 (첫 번째 줄)
                                    let mut log_content = format!("{}\n", header_lines[0]);
                                    
                                    // Content-Length 값 추출
                                    let mut content_length = 0;
                                    let mut is_post = header_lines[0].starts_with("POST ");
                                    
                                    // 필요한 헤더 추출
                                    for line in &header_lines[1..] {
                                        if line.to_lowercase().starts_with("host:") ||
                                           line.to_lowercase().starts_with("user-agent:") ||
                                           line.to_lowercase().starts_with("referer:") ||
                                           line.to_lowercase().starts_with("sec-ch-ua-platform:") {
                                            log_content.push_str(&format!("{}\n", line));
                                        }
                                        
                                        // Content-Length 값 추출
                                        if is_post && line.to_lowercase().starts_with("content-length:") {
                                            log_content.push_str(&format!("{}\n", line));
                                            if let Some(len_str) = line.split(':').nth(1) {
                                                if let Ok(len) = len_str.trim().parse::<usize>() {
                                                    content_length = len;
                                                }
                                            }
                                        }
                                        
                                        // Content-Type 헤더 추가 (POST 요청인 경우)
                                        if is_post && line.to_lowercase().starts_with("content-type:") {
                                            log_content.push_str(&format!("{}\n", line));
                                        }
                                    }
                                    
                                    // POST 요청이고 본문이 있는 경우
                                    if is_post && parts.len() > 1 {
                                        let body = parts[1];
                                        
                                        // 본문 전체를 받았는지 확인
                                        let received_body_len = body.len();
                                        
                                        // 전체 본문을 받았거나 충분한 양을 받았으면 로깅
                                        if content_length == 0 || received_body_len >= content_length || received_body_len >= 4096 {
                                            // 본문 추가 (최대 4096)
                                            let body_preview = if body.len() > 4096 {
                                                format!("{}... (truncated)", &body[0..4096])
                                            } else {
                                                body.to_string()
                                            };
                                            
                                            log_content.push_str("\n"); // 헤더와 본문 사이 빈 줄
                                            log_content.push_str(&body_preview);
                                            
                                            // 로그 파일에 기록
                                            if let Err(e) = file.write_all(format!("{}\n\n", log_content).as_bytes()).await {
                                                error!("[Session:{}] Failed to write to request log file: {}", session_id, e);
                                                request_log_file = None;
                                            }
                                            
                                            // 처리 완료 후 버퍼 초기화
                                            accumulated_data.clear();
                                            processing_request = false;
                                        }
                                    } else {
                                        // GET 요청이나 본문이 없는 요청은 바로 로깅
                                        if let Err(e) = file.write_all(format!("{}\n\n", log_content).as_bytes()).await {
                                            error!("[Session:{}] Failed to write to request log file: {}", session_id, e);
                                            request_log_file = None;
                                        }
                                        
                                        // 처리 완료 후 버퍼 초기화
                                        accumulated_data.clear();
                                        processing_request = false;
                                    }
                                }
                            }
                        }
                        
                        // 버퍼가 너무 커지면 초기화 (메모리 누수 방지)
                        if accumulated_data.len() > BUFFER_SIZE_LARGE {
                            accumulated_data.clear();
                            processing_request = false;
                            debug!("[Session:{}] Accumulated buffer too large, clearing", session_id);
                        }
                    }
                },
                Err(e) => {
                    let err_msg = e.to_string();
                    // TLS close_notify 오류는 debug 레벨로 기록
                    if err_msg.contains("peer closed connection without sending TLS close_notify") {
                        debug!("[Session:{}] Client closed connection: {}", session_id, err_msg);
                    } else {
                        error!("[Session:{}] Error reading from client: {}", session_id, e);
                    }
                    break;
                }
            }
        }
        
        debug!("[Session:{}] Client to server transfer finished: {} bytes", session_id, total_bytes);
        Ok::<u64, io::Error>(total_bytes)
    };
    
    let server_to_client = async {
        let mut buffer = vec![0u8; BUFFER_SIZE_SMALL];
        let mut total_bytes = 0u64;
        
        loop {
            match server_read.read(&mut buffer).await {
                Ok(0) => break, // 연결 종료
                Ok(n) => {
                    // 클라이언트로 데이터 전송
                    if let Err(e) = client_write.write_all(&buffer[0..n]).await {
                        error!("[Session:{}] Failed to write to client: {}", session_id, e);
                        break;
                    }
                    
                    // 메트릭스 업데이트 (실시간)
                    total_bytes += n as u64;
                    metrics.add_tls_bytes_out(n as u64);
                },
                Err(e) => {
                    let err_msg = e.to_string();
                    // TLS close_notify 오류는 debug 레벨로 기록
                    if err_msg.contains("peer closed connection without sending TLS close_notify") {
                        debug!("[Session:{}] Server closed connection: {}", session_id, err_msg);
                    } else {
                        error!("[Session:{}] Error reading from server: {}", session_id, e);
                    }
                    break;
                }
            }
        }
        
        debug!("[Session:{}] Server to client transfer finished: {} bytes", session_id, total_bytes);
        info!("[Session:{}] 최종 서버→클라이언트 전송량: {} bytes", session_id, total_bytes);
        Ok::<u64, io::Error>(total_bytes)
    };
    
    // 양방향 데이터 전송 동시 실행
    let result: Result<(), io::Error> = tokio::select! {
        res = client_to_server => {
            if let Err(e) = res {
                error!("[Session:{}] Client to server transfer failed: {}", session_id, e);
            }
            Ok(())
        },
        res = server_to_client => {
            if let Err(e) = res {
                error!("[Session:{}] Server to client transfer failed: {}", session_id, e);
            }
            Ok(())
        },
    };
    
    // 연결이 종료되었지만, TLS close_notify 오류는 정상 종료로 처리
    if let Err(e) = &result {
        if e.to_string().contains("peer closed connection without sending TLS close_notify") {
            return Ok(());
        }
    }
    
    info!("[Session:{}] TLS proxy completed", session_id);
    Ok(result?)
} 