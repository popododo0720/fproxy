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
use crate::REQUEST_LOGGER;

/// HTTP 스트림 간에 데이터를 전달하고 검사합니다
pub async fn proxy_http_streams(
    client_stream: TcpStream,
    server_stream: TcpStream,
    metrics: Arc<Metrics>,
    session_id: &str,
    request_start_time: Instant,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // 클라이언트 IP 주소 가져오기
    let client_ip = client_stream.peer_addr()
        .map(|addr| addr.ip().to_string())
        .unwrap_or_else(|_| "Unknown IP".to_string());
    
    // 서버 IP 주소 가져오기
    let server_ip = server_stream.peer_addr()
        .map(|addr| addr.ip().to_string())
        .unwrap_or_else(|_| "Unknown IP".to_string());
    
    // HTTP는 항상 표준 터널링 모드 사용 (splice 사용 안 함)
    debug!("[Session:{}] Using standard mode for HTTP streams", session_id);
    let (mut client_read, mut client_write) = tokio::io::split(client_stream);
    let (mut server_read, mut server_write) = tokio::io::split(server_stream);
    
    // 개별 요청 시작 시간 추적을 위한 HashMap
    let request_times: Arc<RwLock<HashMap<u64, Instant>>> = Arc::new(RwLock::new(HashMap::new()));
    
    // HTTP 요청 감지 상태
    let parsing_request = Arc::new(RwLock::new(false));
    let current_request_id = Arc::new(RwLock::new(0_u64));
    
    // 패턴 상수 정의
    let header_end = b"\r\n\r\n";
    let chunk_end = b"\r\n0\r\n\r\n";
    
    // 양방향 데이터 전송 설정
    let client_to_server = {
        let request_times: Arc<RwLock<HashMap<u64, Instant>>> = Arc::clone(&request_times);
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
                        // HTTP 요청 감지 및 파싱 - 해당 부분을 더 효율적으로 구현
                        if !*parsing_request.read().unwrap() && !buffer.is_empty() {
                            let mut headers = [httparse::EMPTY_HEADER; 64];
                            let mut req = httparse::Request::new(&mut headers);
                            
                            if let Ok(status) = req.parse(&buffer) {
                                if status.is_partial() || status.is_complete() {
                                    if let Some(method) = req.method {
                                        if method == "GET" || method == "POST" || method == "PUT" || 
                                           method == "DELETE" || method == "HEAD" || method == "OPTIONS" {
                                            let mut req_id = current_request_id.write().unwrap();
                                            *req_id += 1;
                                            let request_id = *req_id;
                                            
                                            request_times.write().unwrap().insert(request_id, Instant::now());
                                            *parsing_request.write().unwrap() = true;
                                            
                                            debug!("[Session:{}] 새 HTTP 요청 #{} 감지: {} {}", 
                                                  session_id, request_id, method, req.path.unwrap_or(""));
                                            
                                            // DB에 요청 로깅
                                            if let Ok(logger) = REQUEST_LOGGER.try_read() {
                                                let path = req.path.unwrap_or("");
                                                let headers_str = req_buffer.iter().map(|&c| c as char).collect::<String>();
                                                
                                                // 호스트 헤더 추출
                                                let host = headers.iter()
                                                    .find(|h| h.name.eq_ignore_ascii_case("Host"))
                                                    .and_then(|h| std::str::from_utf8(h.value).ok())
                                                    .unwrap_or("unknown_host");
                                                
                                                if let Err(e) = logger.log_async(
                                                    host,
                                                    method,
                                                    path,
                                                    &headers_str,
                                                    None, // 바디는 나중에 처리
                                                    session_id,
                                                    &client_ip, // 클라이언트 IP 추가
                                                    &server_ip, // 서버 IP 추가
                                                    None,
                                                    false,
                                                    false // HTTP는 TLS가 아님
                                                ) {
                                                    warn!("[Session:{}] HTTP 요청 로깅 실패: {}", session_id, e);
                                                }
                                            }
                                            
                                            req_buffer.clear();
                                            req_buffer.put_slice(&buffer);
                                        }
                                    }
                                }
                            }
                        } else if *parsing_request.read().unwrap() {
                            req_buffer.put_slice(&buffer);
                            
                            // 최적화된 패턴 검색 - 고정 크기 패턴에 대해 memmem 라이브러리 활용
                            if header_searcher.search_in(req_buffer.as_ref()).is_some() {
                                debug!("[Session:{}] HTTP 요청 헤더 완료", session_id);
                            }
                        }
                        
                        // 제로 카피 전송 구현
                        // 바이트 슬라이스를 직접 전송
                        let bytes = buffer.freeze();
                        if let Err(e) = server_write.write_all(&bytes).await {
                            error!("[Session:{}] Failed to write to server: {}", session_id, e);
                            break;
                        }
                        
                        // 메트릭스 업데이트 (실시간)
                        total_bytes += n as u64;
                        metrics_clone.add_http_bytes_in(n as u64);
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
        let request_times: Arc<RwLock<HashMap<u64, Instant>>> = Arc::clone(&request_times);
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
                        // 클라이언트로 데이터 전송 - 제로 카피로 구현
                        let bytes = buffer.freeze();
                        if let Err(e) = client_write.write_all(&bytes).await {
                            error!("[Session:{}] Failed to write to client: {}", session_id, e);
                            break;
                        }
                        
                        // 메트릭스 업데이트 (실시간)
                        total_bytes += n as u64;
                        metrics_clone.add_http_bytes_out(n as u64);
                        
                        // HTTP 응답 감지 및 처리
                        if !is_reading_response {
                            let mut headers = [httparse::EMPTY_HEADER; 64];
                            let mut resp = httparse::Response::new(&mut headers);
                            
                            if let Ok(status) = resp.parse(&bytes) {
                                if status.is_partial() || status.is_complete() {
                                    is_reading_response = true;
                                    resp_buffer.clear();
                                    resp_buffer.put_slice(&bytes);
                                    
                                    current_response_for = *current_request_id.read().unwrap();
                                    
                                    debug!("[Session:{}] HTTP 응답 감지, 요청 #{} 처리 중", 
                                          session_id, current_response_for);
                                }
                            }
                        } else {
                            resp_buffer.put_slice(&bytes);
                            
                            // 응답 종료 확인 - 메모리 효율적인 방식으로 구현
                            let mut response_completed = false;
                            let resp_bytes = resp_buffer.as_ref();
                            
                            if let Some(headers_end_pos) = header_searcher.search_in(resp_bytes) {
                                let headers_end_pos = headers_end_pos + 4; // \r\n\r\n 길이 포함
                                
                                let mut headers = [httparse::EMPTY_HEADER; 64];
                                let mut resp = httparse::Response::new(&mut headers);
                                if let Ok(_) = resp.parse(&resp_bytes[..headers_end_pos]) {
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
                                    
                                    // 응답 완료 확인 로직을 최적화
                                    if is_chunked {
                                        if chunk_searcher.search_in(resp_bytes).is_some() {
                                            response_completed = true;
                                            debug!("[Session:{}] Chunked 응답 완료 감지, 요청 #{}", session_id, current_response_for);
                                        }
                                    } else if let Some(len) = content_length {
                                        let body_length = resp_bytes.len() - headers_end_pos;
                                        if body_length >= len {
                                            response_completed = true;
                                            debug!("[Session:{}] Content-Length 응답 완료 감지, 요청 #{}, 길이: {}", 
                                                  session_id, current_response_for, len);
                                        }
                                    } else {
                                        response_completed = true;
                                        debug!("[Session:{}] 헤더 전용 응답 완료 감지, 요청 #{}", session_id, current_response_for);
                                    }
                                    
                                    // 응답 완료 감지 시 처리
                                    if response_completed {
                                        // 응답 시간 측정 및 기록
                                        if let Some(start_time) = request_times.write().unwrap().remove(&current_response_for) {
                                            let duration = start_time.elapsed();
                                            let duration_ms = duration.as_millis() as u64;
                                            
                                            // 응답 시간 메트릭 업데이트
                                            metrics_clone.update_response_time_stats(duration_ms);
                                            
                                            // 응답 시간 로그 기록
                                            if let Ok(logger) = REQUEST_LOGGER.try_read() {
                                                // 여기서는 호스트 정보를 알 수 없으므로 세션 ID만 사용하여 로그 업데이트
                                                debug!("[Session:{}] HTTP 응답 완료, 요청 #{}, 응답 시간: {}ms", 
                                                      session_id, current_response_for, duration_ms);
                                                
                                                // 응답 시간 업데이트 함수 호출
                                                tokio::spawn({
                                                    let logger = logger.clone();
                                                    let session_id = session_id.to_string();
                                                    async move {
                                                        if let Err(e) = logger.update_response_time(&session_id, duration_ms).await {
                                                            debug!("[Session:{}] 응답 시간 업데이트 실패: {}", session_id, e);
                                                        }
                                                    }
                                                });
                                            }
                                        }
                                        
                                        is_reading_response = false;
                                        resp_buffer.clear();
                                        
                                        // 요청 처리 상태 초기화
                                        *parsing_request.write().unwrap() = false;
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
    }
    
    // 세션 전체 시간도 기록 (디버깅 및 모니터링용)
    let total_session_time = request_start_time.elapsed().as_millis() as u64;
    info!("[Session:{}] HTTP 세션 종료, 총 세션 시간: {} ms", session_id, total_session_time);
    
    Ok(())
} 