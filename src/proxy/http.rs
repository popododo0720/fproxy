use std::error::Error;
use std::sync::{Arc, RwLock};
use std::io;
use std::time::Instant;
use std::collections::HashMap;

use log::{debug, error, info, warn};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::metrics::Metrics;
use crate::constants::*;

/// HTTP 스트림 간에 데이터를 전달하고 검사합니다
pub async fn proxy_http_streams(
    client_stream: TcpStream,
    server_stream: TcpStream,
    metrics: Arc<Metrics>,
    session_id: &str,
    request_start_time: Instant,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (mut client_read, mut client_write) = tokio::io::split(client_stream);
    let (mut server_read, mut server_write) = tokio::io::split(server_stream);
    
    // 개별 요청 시작 시간 추적을 위한 HashMap
    let request_times = Arc::new(RwLock::new(HashMap::new()));
    
    // HTTP 요청 감지 상태
    let parsing_request = Arc::new(RwLock::new(false));
    let current_request_id = Arc::new(RwLock::new(0_u64));
    let request_buffer = Arc::new(RwLock::new(Vec::new()));
    
    // 양방향 데이터 전송 설정
    let client_to_server = {
        let request_times = Arc::clone(&request_times);
        let parsing_request = Arc::clone(&parsing_request);
        let current_request_id = Arc::clone(&current_request_id);
        let request_buffer = Arc::clone(&request_buffer);
        let metrics_clone = Arc::clone(&metrics);
        
        async move {
            let mut buffer = vec![0u8; BUFFER_SIZE_SMALL];
            let mut total_bytes = 0u64;
            
            loop {
                match client_read.read(&mut buffer).await {
                    Ok(0) => break, // 연결 종료
                    Ok(n) => {
                        // 서버로 데이터 전송하기 전에 HTTP 요청 감지
                        let data_slice = &buffer[0..n];
                        
                        // HTTP 요청 처리 
                        if let Ok(data_str) = std::str::from_utf8(data_slice) {
                            if !*parsing_request.read().unwrap() {
                                // 새로운 요청 감지 (GET, POST, PUT, DELETE 등으로 시작)
                                if data_str.starts_with("GET ") || data_str.starts_with("POST ") || 
                                   data_str.starts_with("PUT ") || data_str.starts_with("DELETE ") || 
                                   data_str.starts_with("HEAD ") || data_str.starts_with("OPTIONS ") {
                                    
                                    // 새 요청 ID 할당 및 시작 시간 기록
                                    let mut req_id = current_request_id.write().unwrap();
                                    *req_id += 1;
                                    let request_id = *req_id;
                                    
                                    // 요청 시작 시간 기록
                                    request_times.write().unwrap().insert(request_id, Instant::now());
                                    *parsing_request.write().unwrap() = true;
                                    
                                    debug!("[Session:{}] 새 HTTP 요청 #{} 감지: {}", 
                                          session_id, request_id, data_str.lines().next().unwrap_or(""));
                                    
                                    // 요청 버퍼에 데이터 저장
                                    request_buffer.write().unwrap().clear();
                                    request_buffer.write().unwrap().extend_from_slice(data_slice);
                                }
                            } else {
                                // 이미 요청을 처리 중인 경우 버퍼에 추가
                                request_buffer.write().unwrap().extend_from_slice(data_slice);
                                
                                // 요청 종료 확인 (헤더 끝 표시 "\r\n\r\n" 찾기)
                                if let Ok(buffer_str) = std::str::from_utf8(&request_buffer.read().unwrap()) {
                                    if buffer_str.contains("\r\n\r\n") {
                                        debug!("[Session:{}] HTTP 요청 헤더 완료", session_id);
                                    }
                                }
                            }
                        }
                        
                        // 서버로 데이터 전송
                        if let Err(e) = server_write.write_all(&buffer[0..n]).await {
                            error!("[Session:{}] Failed to write to server: {}", session_id, e);
                            break;
                        }
                        
                        // 메트릭스 업데이트 (실시간)
                        total_bytes += n as u64;
                        metrics_clone.add_http_bytes_in(n as u64);
                        
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
                        metrics_clone.add_http_bytes_out(n as u64);
                        
                        // HTTP 응답 감지 및 처리
                        let data_slice = &buffer[0..n];
                        if !is_reading_response {
                            if let Ok(data_str) = std::str::from_utf8(data_slice) {
                                if data_str.starts_with("HTTP/") {
                                    is_reading_response = true;
                                    response_buffer.clear();
                                    response_buffer.extend_from_slice(data_slice);
                                    
                                    // 현재 처리 중인 요청 ID 가져오기
                                    current_response_for = *current_request_id.read().unwrap();
                                    
                                    debug!("[Session:{}] HTTP 응답 감지, 요청 #{} 처리 중", 
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
                                        let mut times = request_times.write().unwrap();
                                        if let Some(start_time) = times.remove(&current_response_for) {
                                            let response_time = start_time.elapsed().as_millis() as u64;
                                            
                                            // 메트릭스 업데이트
                                            metrics_clone.update_response_time_stats(response_time);
                                            
                                            info!("[Session:{}] HTTP 요청 #{} 완료, 응답 시간: {} ms", 
                                                 session_id, current_response_for, response_time);
                                            
                                            // 요청 파싱 상태 초기화
                                            *parsing_request.write().unwrap() = false;
                                        } else {
                                            warn!("[Session:{}] HTTP 요청 #{} 완료되었으나 시작 시간을 찾을 수 없음", 
                                                 session_id, current_response_for);
                                        }
                                        
                                        is_reading_response = false;
                                    }
                                }
                            }
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
        }
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
    
    // 세션 종료 시 모든 미완료 요청에 대한 타임아웃 처리
    let times = request_times.read().unwrap();
    for (req_id, start_time) in times.iter() {
        let response_time = start_time.elapsed().as_millis() as u64;
        warn!("[Session:{}] HTTP 요청 #{} 미완료 종료, 시간: {} ms", session_id, req_id, response_time);
        
        // 미완료 요청은 메트릭스에 포함하지 않음
        // 비정상 종료된 요청은 응답 시간 통계에 포함하지 않는 것이 정확한 모니터링에 도움됨
    }
    
    // 세션 전체 시간도 기록 (디버깅 및 모니터링용)
    let total_session_time = request_start_time.elapsed().as_millis() as u64;
    info!("[Session:{}] HTTP 세션 종료, 총 세션 시간: {} ms", session_id, total_session_time);
    
    Ok(())
} 