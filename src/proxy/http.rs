use std::error::Error;
use std::sync::{Arc};
use std::io;
use std::time::Instant;
use std::collections::HashMap;

use log::{debug, error, info, warn};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;
use bytes::{BytesMut, BufMut};
use httparse;
use memmem::{Searcher, TwoWaySearcher};

use crate::metrics::Metrics;
use crate::constants::*;
use crate::REQUEST_LOGGER;

/// HTTP 응답이 완료되었는지 확인하는 함수
fn is_response_complete(
    resp_bytes: &[u8], 
    headers_end_pos: usize, 
    chunk_searcher: &TwoWaySearcher
) -> (bool, Option<usize>, bool) {
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
            let is_complete = chunk_searcher.search_in(resp_bytes).is_some();
            return (is_complete, None, true);
        } else if let Some(len) = content_length {
            // Content-Length가 있는 경우, 헤더 끝 이후의 본문 길이 확인
            let body_length = resp_bytes.len() - headers_end_pos;
            return (body_length >= len, Some(len), false);
        } else {
            // Content-Length도 없고 chunked도 아닌 경우, 헤더 수신 후 즉시 응답 완료로 처리
            return (true, None, false);
        }
    }
    
    (false, None, false)
}

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
    
    // 개별 요청 시작 시간 추적을 위한 HashMap - tokio::sync::RwLock 사용
    let request_times: Arc<RwLock<HashMap<u64, Instant>>> = Arc::new(RwLock::new(HashMap::new()));
    
    // HTTP 요청 감지 상태 - tokio::sync::RwLock 사용
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
                        if !*parsing_request.read().await && !buffer.is_empty() {
                            let mut headers = [httparse::EMPTY_HEADER; 64];
                            let mut req = httparse::Request::new(&mut headers);
                            
                            if let Ok(status) = req.parse(&buffer) {
                                if status.is_partial() || status.is_complete() {
                                    if let Some(method) = req.method {
                                        if method == "GET" || method == "POST" || method == "PUT" || 
                                           method == "DELETE" || method == "HEAD" || method == "OPTIONS" {
                                            let mut req_id = current_request_id.write().await;
                                            *req_id += 1;
                                            let request_id = *req_id;
                                            
                                            // 요청 시간 기록 - 이 시점에서 요청이 시작된 것으로 간주
                                            request_times.write().await.insert(request_id, Instant::now());
                                            *parsing_request.write().await = true;
                                            
                                            // 요청 정보 로깅 (디버그용)
                                            let path = req.path.unwrap_or("");
                                            debug!("[Session:{}] 새 HTTP 요청 #{} 감지: {} {}", 
                                                  session_id, request_id, method, path);
                                            
                                            // 호스트 헤더 추출
                                            let host = headers.iter()
                                                .find(|h| h.name.eq_ignore_ascii_case("Host"))
                                                .and_then(|h| std::str::from_utf8(h.value).ok())
                                                .unwrap_or("unknown_host");
                                            
                                            // DB에 요청 로깅
                                            if let Ok(logger) = REQUEST_LOGGER.try_read() {
                                                let headers_str = buffer.iter().map(|&c| c as char).collect::<String>();
                                                
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
                                                } else {
                                                    info!("[Session:{}] HTTP 요청 로깅 성공: {} {}", session_id, method, path);
                                                }
                                            }
                                            
                                            req_buffer.clear();
                                            req_buffer.put_slice(&buffer);

                                            // 요청 헤더 끝 확인
                                            if header_searcher.search_in(&buffer).is_some() {
                                                debug!("[Session:{}] HTTP 요청 #{} 헤더 완료", session_id, request_id);
                                                
                                                // Content-Length 확인
                                                let mut has_body = false;
                                                for header in headers.iter() {
                                                    if header.name.is_empty() {
                                                        break;
                                                    }
                                                    
                                                    if header.name.eq_ignore_ascii_case("content-length") {
                                                        if let Ok(len_str) = std::str::from_utf8(header.value) {
                                                            if let Ok(len) = len_str.trim().parse::<usize>() {
                                                                if len > 0 {
                                                                    has_body = true;
                                                                    debug!("[Session:{}] HTTP 요청 #{} 본문 있음 ({}바이트)", 
                                                                          session_id, request_id, len);
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                
                                                // GET/HEAD 요청이거나 본문이 없는 요청은 이 시점에서 완료된 것으로 간주
                                                if method == "GET" || method == "HEAD" || !has_body {
                                                    debug!("[Session:{}] HTTP 요청 #{} 완료 (본문 없음)", session_id, request_id);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        } else if *parsing_request.read().await {
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
            // 초기에는 중형 버퍼로 시작
            let mut resp_buffer = BytesMut::with_capacity(BUFFER_SIZE_MEDIUM);
            let mut is_reading_response = false;
            let mut current_response_for = 0_u64;
            let header_searcher = TwoWaySearcher::new(header_end);
            let chunk_searcher = TwoWaySearcher::new(chunk_end);
            
            loop {
                let mut buffer = BytesMut::with_capacity(BUFFER_SIZE_SMALL);
                
                match server_read.read_buf(&mut buffer).await {
                    Ok(0) => {
                        // 연결 종료 - 현재 진행 중인 응답이 있으면 완료로 처리
                        if is_reading_response {
                            debug!("[Session:{}] 서버 연결 종료로 응답 완료 처리, 요청 #{}", session_id, current_response_for);
                            
                            // 응답 시간 측정 및 기록
                            let duration_ms = {
                                let mut times = request_times.write().await;
                                if let Some(start_time) = times.remove(&current_response_for) {
                                    let duration = start_time.elapsed();
                                    Some(duration.as_millis() as u64)
                                } else {
                                    None
                                }
                            };
                            
                            if let Some(duration_ms) = duration_ms {
                                if let Ok(logger) = REQUEST_LOGGER.try_read() {
                                    info!("[Session:{}] 연결 종료로 인한 HTTP 응답 완료, 요청 #{}, 응답 시간: {}ms", 
                                          session_id, current_response_for, duration_ms);
                                    
                                    // 세션 ID 복사 및 별도 태스크로 실행하여 블로킹 방지
                                    let session_id_str = session_id.to_string();
                                    let duration_ms_copy = duration_ms;
                                    tokio::spawn(async move {
                                        match logger.update_response_time(&session_id_str, duration_ms_copy).await {
                                            Ok(_) => info!("[Session:{}] 연결 종료 시 응답 시간 업데이트 성공: {}ms", session_id_str, duration_ms_copy),
                                            Err(e) => warn!("[Session:{}] 연결 종료 시 응답 시간 업데이트 실패: {}", session_id_str, e)
                                        }
                                    });
                                }
                            } else {
                                warn!("[Session:{}] 연결 종료 시 요청 #{} 시작 시간을 찾을 수 없음", session_id, current_response_for);
                            }
                        } else {
                            debug!("[Session:{}] 서버 연결 종료, 진행 중인 응답 없음", session_id);
                        }
                        break; // 연결 종료
                    },
                    Ok(n) => {
                        // 클라이언트로 데이터 전송 - 제로 카피로 구현
                        let buffer_clone = buffer.clone(); // 먼저 복제
                        let bytes = buffer.freeze(); // 그 다음 freeze
                        
                        // 클라이언트에게 데이터 전송 (원본 데이터)
                        if let Err(e) = client_write.write_all(&bytes).await {
                            error!("[Session:{}] Failed to write to client: {}", session_id, e);
                            break;
                        }
                        
                        // 메트릭스 업데이트 (실시간)
                        total_bytes += n as u64;
                        metrics_clone.add_http_bytes_out(n as u64);
                        
                        // HTTP 응답 감지 및 처리 - 복제된 버퍼 사용
                        if !is_reading_response {
                            // 새로운 응답 감지
                            let mut headers = [httparse::EMPTY_HEADER; 64];
                            let mut resp = httparse::Response::new(&mut headers);
                            
                            if let Ok(status) = resp.parse(&buffer_clone) {
                                if status.is_partial() || status.is_complete() {
                                    is_reading_response = true;
                                    resp_buffer.clear();
                                    resp_buffer.put_slice(&buffer_clone); // 복제된 버퍼 사용
                                    
                                    // 현재 처리 중인 요청 ID 가져오기
                                    current_response_for = *current_request_id.read().await;
                                    
                                    // 응답 상태 코드 로깅
                                    let status_code = resp.code.unwrap_or(0);
                                    info!("[Session:{}] HTTP 응답 감지, 요청 #{}, 상태 코드: {}, 버퍼 크기: {}바이트", 
                                          session_id, current_response_for, status_code, buffer_clone.len());
                                    
                                    // 요청 ID가 0이면 강제로 1로 설정 (첫 번째 요청)
                                    if current_response_for == 0 {
                                        current_response_for = 1;
                                        // 요청 시간 기록 (첫 요청인 경우)
                                        request_times.write().await.insert(current_response_for, Instant::now());
                                        info!("[Session:{}] 요청 ID가 0이어서 1로 설정하고 시간 기록", session_id);
                                    }
                                    
                                    // 헤더 끝 위치 찾기
                                    if let Some(headers_end_pos) = header_searcher.search_in(&buffer_clone) {
                                        let headers_end_pos = headers_end_pos + 4; // \r\n\r\n 길이 포함
                                        info!("[Session:{}] HTTP 헤더 끝 위치: {}, 총 버퍼 크기: {}", 
                                              session_id, headers_end_pos, buffer_clone.len());
                                        
                                        // 본문이 없는 응답인 경우 즉시 완료로 처리
                                        if status_code == 204 || status_code == 304 || 
                                           (status_code >= 100 && status_code < 200) || 
                                           headers_end_pos == buffer_clone.len() {
                                            
                                            // 응답 시간 측정 및 기록 - 락 범위를 최소화
                                            let duration_ms = {
                                                let mut times = request_times.write().await;
                                                if let Some(start_time) = times.remove(&current_response_for) {
                                                    let duration = start_time.elapsed();
                                                    Some(duration.as_millis() as u64)
                                                } else {
                                                    None
                                                }
                                            };
                                            
                                            if let Some(duration_ms) = duration_ms {
                                                info!("[Session:{}] 헤더만 있는 HTTP 응답 즉시 완료 처리, 요청 #{}, 응답 시간: {}ms", 
                                                      session_id, current_response_for, duration_ms);
                                                
                                                // 응답 시간 업데이트 함수 직접 호출
                                                if let Ok(logger) = REQUEST_LOGGER.try_read() {
                                                    let session_id_str = session_id.to_string();
                                                    let duration_ms_copy = duration_ms;
                                                    tokio::spawn(async move {
                                                        match logger.update_response_time(&session_id_str, duration_ms_copy).await {
                                                            Ok(_) => info!("[Session:{}] 헤더 응답 시간 업데이트 성공: {}ms", session_id_str, duration_ms_copy),
                                                            Err(e) => warn!("[Session:{}] 헤더 응답 시간 업데이트 실패: {}", session_id_str, e)
                                                        }
                                                    });
                                                }
                                                
                                                // 요청 파싱 상태 초기화
                                                {
                                                    let mut parsing = parsing_request.write().await;
                                                    *parsing = false;
                                                }
                                                
                                                is_reading_response = false;
                                            }
                                        } else {
                                            // Content-Length 또는 Transfer-Encoding 확인
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
                                                            info!("[Session:{}] HTTP 응답에 Content-Length: {} 감지", 
                                                                  session_id, len);
                                                        }
                                                    }
                                                } else if header.name.eq_ignore_ascii_case("transfer-encoding") {
                                                    if let Ok(value) = std::str::from_utf8(header.value) {
                                                        is_chunked = value.to_lowercase().contains("chunked");
                                                        if is_chunked {
                                                            info!("[Session:{}] HTTP 응답에 chunked 인코딩 감지", session_id);
                                                        }
                                                    }
                                                }
                                            }
                                            
                                            // 헤더 이후 본문 길이 확인
                                            let body_length = buffer_clone.len() - headers_end_pos;
                                            
                                            // Content-Length가 있고 본문이 이미 모두 도착한 경우
                                            if let Some(len) = content_length {
                                                if body_length >= len {
                                                    info!("[Session:{}] 첫 패킷에 모든 본문 수신 완료, 길이: {}/{}", 
                                                          session_id, body_length, len);
                                                    
                                                    // 응답 시간 측정 및 기록
                                                    let duration_ms = {
                                                        let mut times = request_times.write().await;
                                                        if let Some(start_time) = times.remove(&current_response_for) {
                                                            let duration = start_time.elapsed();
                                                            Some(duration.as_millis() as u64)
                                                        } else {
                                                            None
                                                        }
                                                    };
                                                    
                                                    if let Some(duration_ms) = duration_ms {
                                                        info!("[Session:{}] HTTP 응답 첫 패킷에 완료, 요청 #{}, 응답 시간: {}ms", 
                                                              session_id, current_response_for, duration_ms);
                                                        
                                                        // 응답 시간 업데이트
                                                        if let Ok(logger) = REQUEST_LOGGER.try_read() {
                                                            let session_id_str = session_id.to_string();
                                                            let duration_ms_copy = duration_ms;
                                                            tokio::spawn(async move {
                                                                match logger.update_response_time(&session_id_str, duration_ms_copy).await {
                                                                    Ok(_) => info!("[Session:{}] 첫 패킷 응답 시간 업데이트 성공: {}ms", session_id_str, duration_ms_copy),
                                                                    Err(e) => warn!("[Session:{}] 첫 패킷 응답 시간 업데이트 실패: {}", session_id_str, e)
                                                                }
                                                            });
                                                        }
                                                        
                                                        // 요청 파싱 상태 초기화
                                                        {
                                                            let mut parsing = parsing_request.write().await;
                                                            *parsing = false;
                                                        }
                                                        
                                                        is_reading_response = false;
                                                    }
                                                }
                                            } else if is_chunked {
                                                // chunked 인코딩의 경우 종료 패턴 확인
                                                if chunk_searcher.search_in(&buffer_clone).is_some() {
                                                    info!("[Session:{}] 첫 패킷에 chunked 응답 완료 감지", session_id);
                                                    
                                                    // 응답 시간 측정 및 기록
                                                    let duration_ms = {
                                                        let mut times = request_times.write().await;
                                                        if let Some(start_time) = times.remove(&current_response_for) {
                                                            let duration = start_time.elapsed();
                                                            Some(duration.as_millis() as u64)
                                                        } else {
                                                            None
                                                        }
                                                    };
                                                    
                                                    if let Some(duration_ms) = duration_ms {
                                                        info!("[Session:{}] HTTP chunked 응답 첫 패킷에 완료, 요청 #{}, 응답 시간: {}ms", 
                                                              session_id, current_response_for, duration_ms);
                                                        
                                                        // 응답 시간 업데이트
                                                        if let Ok(logger) = REQUEST_LOGGER.try_read() {
                                                            let session_id_str = session_id.to_string();
                                                            let duration_ms_copy = duration_ms;
                                                            tokio::spawn(async move {
                                                                match logger.update_response_time(&session_id_str, duration_ms_copy).await {
                                                                    Ok(_) => info!("[Session:{}] chunked 응답 시간 업데이트 성공: {}ms", session_id_str, duration_ms_copy),
                                                                    Err(e) => warn!("[Session:{}] chunked 응답 시간 업데이트 실패: {}", session_id_str, e)
                                                                }
                                                            });
                                                        }
                                                        
                                                        // 요청 파싱 상태 초기화
                                                        {
                                                            let mut parsing = parsing_request.write().await;
                                                            *parsing = false;
                                                        }
                                                        
                                                        is_reading_response = false;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        } else {
                            // 응답 데이터 축적
                            resp_buffer.put_slice(&buffer_clone); // bytes 대신 buffer_clone 사용
                            
                            // 응답 종료 확인 - 메모리 효율적인 방식으로 구현
                            let mut response_completed = false;
                            let resp_bytes = resp_buffer.as_ref();
                            
                            if let Some(headers_end_pos) = header_searcher.search_in(resp_bytes) {
                                let headers_end_pos = headers_end_pos + 4; // \r\n\r\n 길이 포함
                                
                                // 응답 완료 확인 - 추출된 함수 사용
                                let (is_complete, content_length, is_chunked) = 
                                    is_response_complete(resp_bytes, headers_end_pos, &chunk_searcher);
                                
                                if is_complete {
                                    // 응답 유형에 따른 로그 출력
                                    if is_chunked {
                                        debug!("[Session:{}] Chunked 응답 완료 감지, 요청 #{}", 
                                              session_id, current_response_for);
                                    } else if let Some(len) = content_length {
                                        debug!("[Session:{}] Content-Length 응답 완료 감지, 요청 #{}, 길이: {}", 
                                              session_id, current_response_for, len);
                                    } else {
                                        debug!("[Session:{}] 헤더 전용 응답 완료 감지, 요청 #{}", 
                                              session_id, current_response_for);
                                    }
                                    
                                    response_completed = true;
                                }
                            }
                            
                            // 응답 완료 감지 시 처리
                            if response_completed {
                                // 응답 시간 측정 및 기록 - 락 범위를 최소화
                                let duration_ms = {
                                    let mut times = request_times.write().await;
                                    if let Some(start_time) = times.remove(&current_response_for) {
                                        let duration = start_time.elapsed();
                                        Some(duration.as_millis() as u64)
                                    } else {
                                        None
                                    }
                                };
                                
                                if let Some(duration_ms) = duration_ms {
                                    // 응답 시간 로그 기록
                                    info!("[Session:{}] HTTP 응답 완료, 요청 #{}, 응답 시간: {}ms", 
                                          session_id, current_response_for, duration_ms);
                                    
                                    // 응답 시간 업데이트 함수 직접 호출
                                    if let Ok(logger) = REQUEST_LOGGER.try_read() {
                                        let session_id_str = session_id.to_string();
                                        let duration_ms_copy = duration_ms;
                                        tokio::spawn(async move {
                                            match logger.update_response_time(&session_id_str, duration_ms_copy).await {
                                                Ok(_) => info!("[Session:{}] 응답 시간 업데이트 성공: {}ms", session_id_str, duration_ms_copy),
                                                Err(e) => warn!("[Session:{}] 응답 시간 업데이트 실패: {}", session_id_str, e)
                                            }
                                        });
                                    }
                                }
                                
                                is_reading_response = false;
                                resp_buffer.clear();
                                
                                // 요청 처리 상태 초기화 - 락 범위 최소화
                                {
                                    let mut parsing = parsing_request.write().await;
                                    *parsing = false;
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
    {
        // 범위를 제한하여 times가 .await 호출을 넘어서지 않도록 함
        let times = request_times.read().await;
        let mut unfinished_requests = Vec::new();
        
        // 미완료 요청 정보를 복사
        for (req_id, start_time) in times.iter() {
            let response_time = start_time.elapsed().as_millis() as u64;
            unfinished_requests.push((*req_id, response_time));
            warn!("[Session:{}] HTTP 요청 #{} 미완료 종료, 시간: {} ms", session_id, req_id, response_time);
        }
        
        // times 락 해제
        drop(times);
        
        // 미완료 요청에 대해 응답 시간 기록
        for (req_id, response_time) in unfinished_requests {
            if let Ok(logger) = REQUEST_LOGGER.try_read() {
                // 세션 ID 복사 및 별도 태스크로 실행하여 블로킹 방지
                let session_id_str = session_id.to_string();
                let req_id_copy = req_id;
                let response_time_copy = response_time;
                
                tokio::spawn(async move {
                    match logger.update_response_time(&session_id_str, response_time_copy).await {
                        Ok(_) => debug!("[Session:{}] 미완료 요청 #{} 응답 시간 업데이트 성공: {}ms", 
                                       session_id_str, req_id_copy, response_time_copy),
                        Err(e) => warn!("[Session:{}] 미완료 요청 #{} 응답 시간 업데이트 실패: {}", 
                                       session_id_str, req_id_copy, e)
                    }
                });
            }
        }
    }
    
    // 세션 전체 시간도 기록 (디버깅 및 모니터링용)
    let total_session_time = request_start_time.elapsed().as_millis() as u64;
    info!("[Session:{}] HTTP 세션 종료, 총 세션 시간: {} ms", session_id, total_session_time);
    
    Ok(())
} 