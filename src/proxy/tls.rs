use std::error::Error;
use std::sync::{Arc, RwLock};
use std::io;
use std::time::Instant;
use std::collections::HashMap;

use log::{debug, error, info, warn};
use tokio::net::TcpStream;
use tokio_rustls::{server::TlsStream as ServerTlsStream, client::TlsStream as ClientTlsStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::{BytesMut, BufMut};
use httparse;
use memmem::{Searcher, TwoWaySearcher};

use crate::metrics::Metrics;
use crate::constants::*;
use crate::REQUEST_LOGGER;

// 패턴 상수 정의 - 전역으로 이동하여 매번 생성하지 않도록 함
const HEADER_END_PATTERN: &[u8] = b"\r\n\r\n";
const CHUNK_END_PATTERN: &[u8] = b"\r\n0\r\n\r\n";

// HTTP 요청 메서드 집합
const HTTP_METHODS: [&str; 6] = ["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"];

/// HTTP 요청 정보 파싱하여 헤더와 바디 추출
fn parse_http_request<'a>(buffer: &'a [u8], header_searcher: &TwoWaySearcher) -> (Option<&'a str>, Option<&'a str>, String, Option<String>) {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut req = httparse::Request::new(&mut headers);
    
    if let Ok(status) = req.parse(buffer) {
        if status.is_partial() || status.is_complete() {
            let method = req.method;
            let path = req.path;
            
            // 헤더 추출
            let mut header_str = String::new();
            for header in headers.iter() {
                if header.name.is_empty() {
                    break;
                }
                if !header_str.is_empty() {
                    header_str.push_str("\r\n");
                }
                if let Ok(value) = std::str::from_utf8(header.value) {
                    header_str.push_str(&format!("{}: {}", header.name, value));
                }
            }
            
            // 본문 추출 시도
            let mut body = None;
            if let Some(header_end_pos) = header_searcher.search_in(buffer) {
                let body_start = header_end_pos + 4; // "\r\n\r\n" 길이
                if body_start < buffer.len() {
                    if let Ok(body_str) = std::str::from_utf8(&buffer[body_start..]) {
                        body = Some(body_str.to_string());
                    }
                }
            }
            
            return (method, path, header_str, body);
        }
    }
    
    (None, None, String::new(), None)
}

/// 응답의 완료 여부 확인
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

/// TLS 스트림 간에 데이터를 전달하고 검사합니다
pub async fn proxy_tls_streams(
    client_stream: ServerTlsStream<TcpStream>,
    server_stream: ClientTlsStream<TcpStream>,
    metrics: Arc<Metrics>,
    session_id: &str,
    host: &str,
    request_start_time: Instant,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // 클라이언트 IP 주소 가져오기 (스트림 분할 전에)
    let client_ip = client_stream.get_ref().0.peer_addr()
        .map(|addr| addr.ip().to_string())
        .unwrap_or_else(|_| "Unknown IP".to_string());
    
    // 서버 IP 주소 가져오기
    let server_ip = server_stream.get_ref().0.peer_addr()
        .map(|addr| addr.ip().to_string())
        .unwrap_or_else(|_| "Unknown IP".to_string());
    
    // 연결된 IP 주소 로깅
    info!("[Session:{}] Connected to IP: {} for host: {}, client IP: {}", 
         session_id, server_ip, host, client_ip);
    
    // 양방향 데이터 전송 및 검사 로직 구현
    let (mut client_read, mut client_write) = tokio::io::split(client_stream);
    
    let client_ip_clone = client_ip.clone();
    let server_ip_clone = server_ip.clone();
        
    let (mut server_read, mut server_write) = tokio::io::split(server_stream);
    
    // 개별 요청 시작 시간 추적을 위한 HashMap
    let request_times = Arc::new(RwLock::new(HashMap::with_capacity(16))); // 용량 미리 할당
    
    // HTTP 요청 감지 상태
    let parsing_request = Arc::new(RwLock::new(false));
    let current_request_id = Arc::new(RwLock::new(0_u64));
    
    // 바이트 카운터 초기화
    let client_to_server = {
        let request_times = Arc::clone(&request_times);
        let parsing_request = Arc::clone(&parsing_request);
        let current_request_id = Arc::clone(&current_request_id);
        let metrics_clone = Arc::clone(&metrics);
        let client_ip = client_ip_clone.clone();
        let server_ip = server_ip_clone.clone();
        
        async move {
            let mut total_bytes = 0u64;
            let mut req_buffer = BytesMut::with_capacity(BUFFER_SIZE_MEDIUM);
            
            // 패턴 검색기 초기화 - 각 클로저에서 별도로 생성
            let header_searcher = TwoWaySearcher::new(HEADER_END_PATTERN);
            
            // 버퍼 재사용을 위한 초기화
            let mut buffer = BytesMut::with_capacity(BUFFER_SIZE_MEDIUM);
            
            loop {
                buffer.clear(); // 버퍼 재사용 
                
                match client_read.read_buf(&mut buffer).await {
                    Ok(0) => break, // 연결 종료
                    Ok(n) => {
                        // 요청 버퍼가 비어있고 새로운 요청이 시작되는 경우
                        if !*parsing_request.read().unwrap() && !buffer.is_empty() {
                            let (method, path, header_str, body) = parse_http_request(&buffer, &header_searcher);
                            
                            if let Some(method_str) = method {
                                if HTTP_METHODS.contains(&method_str) {
                                    // 새 요청 ID 할당 및 시작 시간 기록
                                    let mut req_id = current_request_id.write().unwrap();
                                    *req_id += 1;
                                    let request_id = *req_id;
                                    
                                    // 요청 시작 시간 기록
                                    request_times.write().unwrap().insert(request_id, Instant::now());
                                    *parsing_request.write().unwrap() = true;
                                    
                                    debug!("[Session:{}] 새 HTTPS 요청 #{} 감지: {} {}", 
                                          session_id, request_id, method_str, path.unwrap_or(""));

                                    // DB에 요청 로깅
                                    if let Ok(logger) = REQUEST_LOGGER.try_read() {
                                        match logger.log_async(
                                            host,
                                            method_str,
                                            path.unwrap_or(""),
                                            &header_str,
                                            body,
                                            session_id,
                                            &client_ip, // 클라이언트 IP 추가
                                            &server_ip, // 서버 IP 추가
                                            None,
                                            false,
                                            true // TLS 요청임을 표시
                                        ) {
                                            Ok(_) => debug!("[Session:{}] HTTPS 요청 로깅 성공", session_id),
                                            Err(e) => warn!("[Session:{}] HTTPS 요청 로깅 실패: {}", session_id, e)
                                        }
                                    }
                                    
                                    // 요청 버퍼 초기화
                                    req_buffer.clear();
                                    req_buffer.put_slice(&buffer);
                                }
                            }
                        } else if *parsing_request.read().unwrap() {
                            // 이미 요청을 처리 중인 경우 버퍼에 추가
                            req_buffer.put_slice(&buffer);
                            
                            // 요청 헤더 완료 여부 확인 (속도 향상을 위해 바이트 패턴 검색)
                            if header_searcher.search_in(req_buffer.as_ref()).is_some() {
                                debug!("[Session:{}] HTTPS 요청 헤더 완료", session_id);
                            }
                        }
                        
                        // 서버로 데이터 전송
                        if let Err(e) = server_write.write_all(&buffer).await {
                            error!("[Session:{}] Failed to write to server: {}", session_id, e);
                            break;
                        }
                        
                        // 메트릭스 업데이트 (실시간)
                        total_bytes += n as u64;
                        metrics_clone.add_tls_bytes_in(n as u64);
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
        let _client_ip = client_ip_clone;
        let _server_ip = server_ip_clone;
        
        async move {
            let mut total_bytes = 0u64;
            let mut resp_buffer = BytesMut::with_capacity(BUFFER_SIZE_MEDIUM);
            let mut is_reading_response = false;
            let mut current_response_for = 0_u64;
            
            // 패턴 검색기 초기화 - 각 클로저에서 별도로 생성
            let header_searcher = TwoWaySearcher::new(HEADER_END_PATTERN);
            let chunk_searcher = TwoWaySearcher::new(CHUNK_END_PATTERN);
            
            // 버퍼 재사용을 위한 초기화
            let mut buffer = BytesMut::with_capacity(BUFFER_SIZE_SMALL);
            
            loop {
                buffer.clear(); // 버퍼 재사용
                
                match server_read.read_buf(&mut buffer).await {
                    Ok(0) => break, // 연결 종료
                    Ok(n) => {
                        // 클라이언트로 데이터 전송 - 오류 시 즉시 연결 종료
                        if let Err(e) = client_write.write_all(&buffer).await {
                            error!("[Session:{}] Failed to write to client: {}", session_id, e);
                            break;
                        }
                        
                        // 메트릭스 업데이트 (실시간)
                        total_bytes += n as u64;
                        metrics_clone.add_tls_bytes_out(n as u64);
                        
                        // HTTP 응답 감지 및 처리
                        if !is_reading_response {
                            // 새로운 응답 감지
                            let mut headers = [httparse::EMPTY_HEADER; 64];
                            let mut resp = httparse::Response::new(&mut headers);
                            
                            if let Ok(status) = resp.parse(&buffer) {
                                if status.is_partial() || status.is_complete() {
                                    is_reading_response = true;
                                    resp_buffer.clear();
                                    resp_buffer.put_slice(&buffer);
                                    
                                    // 현재 처리 중인 요청 ID 가져오기
                                    current_response_for = *current_request_id.read().unwrap();
                                    
                                    debug!("[Session:{}] HTTPS 응답 감지, 요청 #{} 처리 중", 
                                          session_id, current_response_for);
                                }
                            }
                        } else {
                            // 응답 데이터 축적
                            resp_buffer.put_slice(&buffer);
                            
                            // 헤더 끝 위치 찾기
                            if let Some(headers_end_pos) = header_searcher.search_in(resp_buffer.as_ref()) {
                                let headers_end_pos = headers_end_pos + 4; // \r\n\r\n 길이 포함
                                
                                // 응답 완료 확인 - 추출된 함수 사용
                                let (is_complete, content_length, is_chunked) = 
                                    is_response_complete(resp_buffer.as_ref(), headers_end_pos, &chunk_searcher);
                                
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
                                    
                                    // 응답 시간 계산 - 락 범위를 최소화
                                    let response_time = {
                                        let mut times = request_times.write().unwrap();
                                        if let Some(start_time) = times.remove(&current_response_for) {
                                            let rt = start_time.elapsed().as_millis() as u64;
                                            Some(rt)
                                        } else {
                                            None
                                        }
                                    };
                                    
                                    if let Some(response_time) = response_time {
                                        // 응답 시간 로그 기록
                                        info!("[Session:{}] HTTPS 요청 #{} 완료, 응답 시간: {} ms", 
                                             session_id, current_response_for, response_time);
                                        
                                        // DB에 응답 시간 기록
                                        if let Ok(logger) = REQUEST_LOGGER.try_read() {
                                            // 응답 시간 업데이트 함수 직접 호출
                                            let session_id_str = session_id.to_string(); // 세션 ID 복사
                                            let response_time_copy = response_time;
                                            
                                            tokio::spawn(async move {
                                                match logger.update_response_time(&session_id_str, response_time_copy).await {
                                                    Ok(_) => debug!("[Session:{}] 응답 시간 업데이트 성공: {}ms", session_id_str, response_time_copy),
                                                    Err(e) => warn!("[Session:{}] 응답 시간 업데이트 실패: {}", session_id_str, e)
                                                }
                                            });
                                        }
                                        
                                        // 요청 파싱 상태 초기화 - 락 범위 최소화
                                        {
                                            let mut parsing = parsing_request.write().unwrap();
                                            *parsing = false;
                                        }
                                    } else {
                                        warn!("[Session:{}] HTTPS 요청 #{} 완료되었으나 시작 시간을 찾을 수 없음", 
                                             session_id, current_response_for);
                                    }
                                    
                                    is_reading_response = false;
                                    resp_buffer.clear(); // 메모리 효율성을 위해 버퍼 즉시 비우기
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
    let times = request_times.read().unwrap();
    for (req_id, start_time) in times.iter() {
        let response_time = start_time.elapsed().as_millis() as u64;
        warn!("[Session:{}] HTTPS 요청 #{} 미완료 종료, 시간: {} ms", session_id, req_id, response_time);
        
        // 미완료 요청에 대해 응답 시간 기록
        if let Ok(logger) = REQUEST_LOGGER.try_read() {
            // 세션 ID 복사 및 별도 태스크로 실행하여 블로킹 방지
            let session_id_str = session_id.to_string();
            let req_id_copy = *req_id;
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