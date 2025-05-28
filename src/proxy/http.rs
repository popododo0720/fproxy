use std::error::Error;
use std::sync::{Arc, RwLock};
use std::io;
use std::time::Instant;
use std::collections::HashMap;

use log::{debug, error, info, warn};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::{BytesMut, BufMut};
use httparse;
use memmem::{Searcher, TwoWaySearcher};

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
    
    // 패턴 상수 정의
    let header_end = b"\r\n\r\n";
    let chunk_end = b"\r\n0\r\n\r\n";
    
    // 양방향 데이터 전송 설정
    let client_to_server = {
        let request_times = Arc::clone(&request_times);
        let parsing_request = Arc::clone(&parsing_request);
        let current_request_id = Arc::clone(&current_request_id);
        let metrics_clone = Arc::clone(&metrics);
        
        async move {
            let mut total_bytes = 0u64;
            let mut req_buffer = BytesMut::with_capacity(BUFFER_SIZE_SMALL);
            let header_searcher = TwoWaySearcher::new(header_end);
            
            loop {
                let mut buffer = BytesMut::with_capacity(BUFFER_SIZE_SMALL);
                
                match client_read.read_buf(&mut buffer).await {
                    Ok(0) => break, // 연결 종료
                    Ok(n) => {
                        // HTTP 요청 감지 및 파싱
                        let mut headers = [httparse::EMPTY_HEADER; 64];
                        let mut req = httparse::Request::new(&mut headers);
                        
                        // 요청 버퍼가 비어있고 새로운 요청이 시작되는 경우
                        if !*parsing_request.read().unwrap() && !buffer.is_empty() {
                            if let Ok(status) = req.parse(&buffer) {
                                if status.is_partial() || status.is_complete() {
                                    // 새 요청 감지
                                    if let Some(method) = req.method {
                                        if method == "GET" || method == "POST" || method == "PUT" || 
                                           method == "DELETE" || method == "HEAD" || method == "OPTIONS" {
                                            // 새 요청 ID 할당 및 시작 시간 기록
                                            let mut req_id = current_request_id.write().unwrap();
                                            *req_id += 1;
                                            let request_id = *req_id;
                                            
                                            // 요청 시작 시간 기록
                                            request_times.write().unwrap().insert(request_id, Instant::now());
                                            *parsing_request.write().unwrap() = true;
                                            
                                            debug!("[Session:{}] 새 HTTP 요청 #{} 감지: {} {}", 
                                                  session_id, request_id, method, req.path.unwrap_or(""));
                                            
                                            // 요청 버퍼 초기화
                                            req_buffer.clear();
                                            req_buffer.put_slice(&buffer);
                                        }
                                    }
                                }
                            }
                        } else if *parsing_request.read().unwrap() {
                            // 이미 요청을 처리 중인 경우 버퍼에 추가
                            req_buffer.put_slice(&buffer);
                            
                            // 요청 헤더 완료 여부 확인 (속도 향상을 위해 바이트 패턴 검색)
                            let buf_bytes = req_buffer.as_ref();
                            if header_searcher.search_in(buf_bytes).is_some() {
                                debug!("[Session:{}] HTTP 요청 헤더 완료", session_id);
                            }
                        }
                        
                        // 서버로 데이터 전송
                        if let Err(e) = server_write.write_all(&buffer).await {
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
            let mut total_bytes = 0u64;
            let mut resp_buffer = BytesMut::with_capacity(BUFFER_SIZE_MEDIUM);
            let mut is_reading_response = false;
            let mut current_response_for = 0_u64;
            let header_searcher = TwoWaySearcher::new(header_end);
            let chunk_searcher = TwoWaySearcher::new(chunk_end);
            
            loop {
                let mut buffer = BytesMut::with_capacity(BUFFER_SIZE_SMALL);
                
                match server_read.read_buf(&mut buffer).await {
                    Ok(0) => break, // 연결 종료
                    Ok(n) => {
                        // 클라이언트로 데이터 전송
                        if let Err(e) = client_write.write_all(&buffer).await {
                            error!("[Session:{}] Failed to write to client: {}", session_id, e);
                            break;
                        }
                        
                        // 메트릭스 업데이트 (실시간)
                        total_bytes += n as u64;
                        metrics_clone.add_http_bytes_out(n as u64);
                        
                        // HTTP 응답 감지 및 처리
                        let mut headers = [httparse::EMPTY_HEADER; 64];
                        let mut resp = httparse::Response::new(&mut headers);
                        
                        if !is_reading_response {
                            // 새로운 응답 감지
                            if let Ok(status) = resp.parse(&buffer) {
                                if status.is_partial() || status.is_complete() {
                                    is_reading_response = true;
                                    resp_buffer.clear();
                                    resp_buffer.put_slice(&buffer);
                                    
                                    // 현재 처리 중인 요청 ID 가져오기
                                    current_response_for = *current_request_id.read().unwrap();
                                    
                                    debug!("[Session:{}] HTTP 응답 감지, 요청 #{} 처리 중", 
                                          session_id, current_response_for);
                                }
                            }
                        } else {
                            // 응답 데이터 축적
                            resp_buffer.put_slice(&buffer);
                            
                            // 응답 종료 확인
                            let mut response_completed = false;
                            let resp_bytes = resp_buffer.as_ref();
                            
                            // 헤더 끝 위치 찾기
                            if let Some(headers_end_pos) = header_searcher.search_in(resp_bytes) {
                                let headers_end_pos = headers_end_pos + 4; // \r\n\r\n 길이 포함
                                
                                // 헤더 파싱
                                let mut headers = [httparse::EMPTY_HEADER; 64];
                                let mut resp = httparse::Response::new(&mut headers);
                                if let Ok(_) = resp.parse(&resp_bytes[..headers_end_pos]) {
                                    // Content-Length 또는 Transfer-Encoding 찾기
                                    let mut content_length = None;
                                    let mut is_chunked = false;
                                    
                                    for header in headers.iter() {
                                        if header.name.is_empty() {
                                            break;
                                        }
                                        
                                        if header.name.eq_ignore_ascii_case("content-length") {
                                            if let Ok(len_str) = std::str::from_utf8(header.value) {
                                                if let Ok(len) = len_str.trim().parse::<usize>() {
                                                    content_length = Some(len);
                                                }
                                            }
                                        } else if header.name.eq_ignore_ascii_case("transfer-encoding") {
                                            if let Ok(value) = std::str::from_utf8(header.value) {
                                                is_chunked = value.to_lowercase().contains("chunked");
                                            }
                                        }
                                    }
                                    
                                    // 응답 완료 확인
                                    if is_chunked {
                                        // chunked 인코딩의 경우 마지막 청크 패턴 확인
                                        if chunk_searcher.search_in(resp_bytes).is_some() {
                                            response_completed = true;
                                            debug!("[Session:{}] Chunked 응답 완료 감지, 요청 #{}", session_id, current_response_for);
                                        }
                                    } else if let Some(len) = content_length {
                                        // Content-Length가 있는 경우, 헤더 끝 이후의 본문 길이 확인
                                        let body_length = resp_bytes.len() - headers_end_pos;
                                        if body_length >= len {
                                            response_completed = true;
                                            debug!("[Session:{}] Content-Length 응답 완료 감지, 요청 #{}, 길이: {}", 
                                                  session_id, current_response_for, len);
                                        }
                                    } else {
                                        // Content-Length도 없고 chunked도 아닌 경우, 헤더 수신 후 즉시 응답 완료로 처리
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