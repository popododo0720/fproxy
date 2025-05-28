use std::error::Error;
use std::sync::Arc;
use std::io;
use std::time::Instant;
use std::collections::HashMap;
use std::sync::Mutex;

use log::{debug, error, info, warn};
use tokio::net::TcpStream;
use tokio_rustls::{server::TlsStream as ServerTlsStream, client::TlsStream as ClientTlsStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::metrics::Metrics;
use crate::constants::*;
use crate::acl::request_logger::RequestLogger;

/// TLS 스트림 간에 데이터를 전달하고 검사합니다
pub async fn proxy_tls_streams(
    client_stream: ServerTlsStream<TcpStream>,
    server_stream: ClientTlsStream<TcpStream>,
    metrics: Arc<Metrics>,
    session_id: &str,
    host: &str,
    request_start_time: Instant,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // 양방향 데이터 전송 및 검사 로직 구현
    let (mut client_read, mut client_write) = tokio::io::split(client_stream);
    
    // 연결된 IP 주소 가져오기
    let connected_ip = server_stream.get_ref().0.peer_addr()
        .map(|addr| addr.ip().to_string())
        .unwrap_or_else(|_| "Unknown IP".to_string());
        
    let (mut server_read, mut server_write) = tokio::io::split(server_stream);
    
    // 로거 인스턴스 생성
    let request_logger = RequestLogger::new();
    
    // 개별 요청 시작 시간 추적을 위한 HashMap
    let request_times = Arc::new(Mutex::new(HashMap::new()));
    
    // HTTP 요청 감지 상태
    let parsing_request = Arc::new(Mutex::new(false));
    let current_request_id = Arc::new(Mutex::new(0_u64));
    let request_buffer = Arc::new(Mutex::new(Vec::new()));
    
    // 바이트 카운터 초기화
    let client_to_server = {
        let request_times = Arc::clone(&request_times);
        let parsing_request = Arc::clone(&parsing_request);
        let current_request_id = Arc::clone(&current_request_id);
        let request_buffer = Arc::clone(&request_buffer);
        let metrics_clone = Arc::clone(&metrics);
        
        async move {
            let mut buffer = vec![0u8; BUFFER_SIZE_MEDIUM];
            let mut total_bytes = 0u64;
            
            // 요청 저장을 위한 파일 생성
            let mut request_log_file = RequestLogger::create_log_file(host, session_id).await;
            
            // 연결된 IP 주소 로깅
            if let Some(file) = &mut request_log_file {
                let ip_line = format!("# Connected to IP: {}\n# --------------------\n", connected_ip);
                if let Err(e) = file.write_all(ip_line.as_bytes()).await {
                    error!("[Session:{}] Failed to write connected IP to log file: {}", session_id, e);
                }
                info!("[Session:{}] Connected to IP: {} for host: {}", session_id, connected_ip, host);
            }
            
            // 누적 버퍼 (HTTP 요청이 여러 패킷으로 나뉘어 올 경우를 대비)
            let mut accumulated_data = Vec::new();
            let mut old_processing_request = false;
            
            loop {
                match client_read.read(&mut buffer).await {
                    Ok(0) => break, // 연결 종료
                    Ok(n) => {
                        // 데이터 슬라이스 가져오기
                        let data_slice = &buffer[0..n];
                        
                        // HTTP 요청 감지 및 처리
                        if let Ok(data_str) = std::str::from_utf8(data_slice) {
                            if !*parsing_request.lock().unwrap() {
                                // 새로운 요청 감지 (GET, POST, PUT, DELETE 등으로 시작)
                                if data_str.starts_with("GET ") || data_str.starts_with("POST ") || 
                                   data_str.starts_with("PUT ") || data_str.starts_with("DELETE ") || 
                                   data_str.starts_with("HEAD ") || data_str.starts_with("OPTIONS ") {
                                    
                                    // 새 요청 ID 할당 및 시작 시간 기록
                                    let mut req_id = current_request_id.lock().unwrap();
                                    *req_id += 1;
                                    let request_id = *req_id;
                                    
                                    // 요청 시작 시간 기록
                                    request_times.lock().unwrap().insert(request_id, Instant::now());
                                    *parsing_request.lock().unwrap() = true;
                                    
                                    debug!("[Session:{}] 새 HTTPS 요청 #{} 감지: {}", 
                                          session_id, request_id, data_str.lines().next().unwrap_or(""));
                                    
                                    // 요청 버퍼에 데이터 저장
                                    request_buffer.lock().unwrap().clear();
                                    request_buffer.lock().unwrap().extend_from_slice(data_slice);
                                }
                            } else {
                                // 이미 요청을 처리 중인 경우 버퍼에 추가
                                request_buffer.lock().unwrap().extend_from_slice(data_slice);
                                
                                // 요청 종료 확인 (헤더 끝 표시 "\r\n\r\n" 찾기)
                                if let Ok(buffer_str) = std::str::from_utf8(&request_buffer.lock().unwrap()) {
                                    if buffer_str.contains("\r\n\r\n") {
                                        debug!("[Session:{}] HTTPS 요청 헤더 완료", session_id);
                                    }
                                }
                            }
                        }
                        
                        // 서버로 데이터 전송
                        if let Err(e) = server_write.write_all(data_slice).await {
                            error!("[Session:{}] Failed to write to server: {}", session_id, e);
                            break;
                        }
                        
                        // 메트릭스 업데이트 (실시간)
                        total_bytes += n as u64;
                        metrics_clone.add_tls_bytes_in(n as u64);
                        
                        // 기존 RequestLogger에서도 호출 (로그 기록용)
                        accumulated_data.extend_from_slice(data_slice);
                        request_logger.process_request_data(
                            data_slice, 
                            &mut request_log_file, 
                            &mut accumulated_data, 
                            &mut old_processing_request, 
                            session_id,
                            session_id, // request_id로 session_id 사용
                            host
                        ).await;
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
        }
    };
    
    let server_to_client = {
        let request_times = Arc::clone(&request_times);
        let parsing_request = Arc::clone(&parsing_request);
        let current_request_id = Arc::clone(&current_request_id);
        let metrics_clone = Arc::clone(&metrics);
        
        async move {
            let mut buffer = vec![0u8; BUFFER_SIZE_SMALL];
            let mut total_bytes = 0u64;
            let mut response_buffer = Vec::new();
            let mut is_reading_response = false;
            let mut current_response_for = 0_u64;
            
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
                        metrics_clone.add_tls_bytes_out(n as u64);
                        
                        // HTTP 응답 감지 및 처리
                        let data_slice = &buffer[0..n];
                        if !is_reading_response {
                            if let Ok(data_str) = std::str::from_utf8(data_slice) {
                                if data_str.starts_with("HTTP/") {
                                    is_reading_response = true;
                                    response_buffer.clear();
                                    response_buffer.extend_from_slice(data_slice);
                                    
                                    // 현재 처리 중인 요청 ID 가져오기
                                    current_response_for = *current_request_id.lock().unwrap();
                                    
                                    debug!("[Session:{}] HTTPS 응답 감지, 요청 #{} 처리 중", 
                                          session_id, current_response_for);
                                }
                            }
                        } else {
                            // 응답 데이터 축적
                            response_buffer.extend_from_slice(data_slice);
                            
                            // 응답 종료 확인
                            if let Ok(resp_str) = std::str::from_utf8(&response_buffer) {
                                // 응답 헤더 확인
                                if resp_str.contains("\r\n\r\n") {
                                    // 헤더 분석을 위해 헤더와 본문 분리
                                    let parts: Vec<&str> = resp_str.split("\r\n\r\n").collect();
                                    let headers = parts[0].to_lowercase();
                                    
                                    // Content-Length 또는 Transfer-Encoding 확인
                                    let mut response_completed = false;
                                    let mut content_length = None;
                                    let is_chunked = headers.contains("transfer-encoding: chunked");
                                    
                                    // Content-Length 헤더 값 추출
                                    if !is_chunked {
                                        for line in headers.lines() {
                                            if line.starts_with("content-length:") {
                                                if let Some(len_str) = line.split(':').nth(1) {
                                                    if let Ok(len) = len_str.trim().parse::<usize>() {
                                                        content_length = Some(len);
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    
                                    // 응답 완료 확인
                                    if is_chunked {
                                        // chunked 인코딩의 경우 마지막 청크 패턴 확인
                                        if resp_str.contains("\r\n0\r\n\r\n") {
                                            response_completed = true;
                                            debug!("[Session:{}] Chunked 응답 완료 감지, 요청 #{}", session_id, current_response_for);
                                        }
                                    } else if let Some(len) = content_length {
                                        // Content-Length가 있는 경우, 헤더 끝 이후의 본문 길이 확인
                                        if parts.len() > 1 {
                                            let body = parts[1];
                                            if body.len() >= len {
                                                response_completed = true;
                                                debug!("[Session:{}] Content-Length 응답 완료 감지, 요청 #{}, 길이: {}", 
                                                      session_id, current_response_for, len);
                                            }
                                        }
                                    } else {
                                        // Content-Length도 없고 chunked도 아닌 경우, 헤더 수신 후 즉시 응답 완료로 처리
                                        // (HTTP/1.0 또는 헤더만 있는 응답)
                                        response_completed = true;
                                        debug!("[Session:{}] 헤더 전용 응답 완료 감지, 요청 #{}", session_id, current_response_for);
                                    }
                                    
                                    // 응답이 완료된 경우
                                    if response_completed {
                                        // 응답 시간 계산
                                        let mut times = request_times.lock().unwrap();
                                        if let Some(start_time) = times.remove(&current_response_for) {
                                            let response_time = start_time.elapsed().as_millis() as u64;
                                            
                                            // 메트릭스 업데이트
                                            metrics_clone.update_response_time_stats(response_time);
                                            
                                            info!("[Session:{}] HTTPS 요청 #{} 완료, 응답 시간: {} ms", 
                                                 session_id, current_response_for, response_time);
                                            
                                            // 요청 파싱 상태 초기화
                                            *parsing_request.lock().unwrap() = false;
                                        } else {
                                            warn!("[Session:{}] HTTPS 요청 #{} 완료되었으나 시작 시간을 찾을 수 없음", 
                                                 session_id, current_response_for);
                                        }
                                        
                                        is_reading_response = false;
                                    }
                                }
                            }
                        }
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
        }
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
    
    // 세션 종료 시 모든 미완료 요청에 대한 타임아웃 처리
    let times = request_times.lock().unwrap();
    for (req_id, start_time) in times.iter() {
        let response_time = start_time.elapsed().as_millis() as u64;
        warn!("[Session:{}] HTTPS 요청 #{} 미완료 종료, 시간: {} ms", session_id, req_id, response_time);
        
        // 미완료 요청은 타임아웃으로 처리하되 메트릭스에는 포함하지 않음
        // 필요한 경우 메트릭스에 별도 필드를 추가하여 미완료된 요청 수를 추적할 수 있음
    }
    
    // 세션 전체 시간도 기록 (디버깅 및 모니터링용)
    let total_session_time = request_start_time.elapsed().as_millis() as u64;
    info!("[Session:{}] TLS 세션 종료, 총 세션 시간: {} ms", session_id, total_session_time);
    
    // 연결이 종료되었지만, TLS close_notify 오류는 정상 종료로 처리
    if let Err(e) = &result {
        if e.to_string().contains("peer closed connection without sending TLS close_notify") {
            return Ok(());
        }
    }
    
    Ok(result?)
} 