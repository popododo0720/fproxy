use std::error::Error;
use std::sync::{Arc, RwLock};
use std::io;
use std::time::Instant;
use std::collections::HashMap;
use std::os::unix::io::{RawFd, AsRawFd};

use log::{debug, error, info, warn};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::{BytesMut, BufMut};
use httparse;
use memmem::{Searcher, TwoWaySearcher};
use nix::unistd::{close, pipe};
use libc;

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
    if USE_SPLICE {
        return proxy_http_streams_splice(client_stream, server_stream, metrics, session_id, request_start_time).await;
    }

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
                                    
                                    if response_completed {
                                        // 응답 시간 계산
                                        let mut times = request_times.write().unwrap();
                                        if let Some(start_time) = times.remove(&current_response_for) {
                                            let response_time = start_time.elapsed().as_millis() as u64;
                                            
                                            metrics_clone.update_response_time_stats(response_time);
                                            
                                            info!("[Session:{}] HTTP 요청 #{} 완료, 응답 시간: {} ms", 
                                                 session_id, current_response_for, response_time);
                                            
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
    }
    
    // 세션 전체 시간도 기록 (디버깅 및 모니터링용)
    let total_session_time = request_start_time.elapsed().as_millis() as u64;
    info!("[Session:{}] HTTP 세션 종료, 총 세션 시간: {} ms", session_id, total_session_time);
    
    Ok(())
}

/// Linux에서 splice 시스템 콜을 사용하여 데이터 전송을 최적화하는 함수
async fn proxy_http_streams_splice(
    client_stream: TcpStream, 
    server_stream: TcpStream,
    metrics: Arc<Metrics>,
    session_id: &str,
    request_start_time: Instant,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    debug!("[Session:{}] Using kernel splice for HTTP streams", session_id);
    
    // TCP 소켓 최적화 - 네이글 알고리즘 비활성화 및 퀵 ACK 활성화
    set_tcp_socket_options(&client_stream, &server_stream)?;
    
    // 클라이언트와 서버 파일 디스크립터 가져오기
    let client_fd = client_stream.as_raw_fd();
    let server_fd = server_stream.as_raw_fd();
    
    // 클라이언트->서버, 서버->클라이언트 각각의 파이프 생성 - 비동기적 파이프 생성
    let (pipe_c2s_read, pipe_c2s_write) = create_nonblocking_pipe()?;
    let (pipe_s2c_read, pipe_s2c_write) = create_nonblocking_pipe()?;
    
    // 소켓 버퍼 사이즈 최적화
    optimize_socket_buffers(client_fd, server_fd)?;
    
    // 새로운 스레드에서 데이터 전송을 처리 (tokio worker 스레드에서 블로킹 호출을 피하기 위함)
    let metrics_clone = Arc::clone(&metrics);
    let session_id_clone = session_id.to_string();
    
    let client_to_server_handle = std::thread::spawn(move || {
        let mut total_bytes = 0u64;
        let mut max_splice_size = BUFFER_SIZE_SMALL; // 초기 크기
        
        loop {
            // 클라이언트로부터 파이프로 데이터 전송
            #[cfg(target_os = "linux")]
            let splice_result = unsafe {
                libc::splice(
                    client_fd,
                    std::ptr::null_mut(),
                    pipe_c2s_write,
                    std::ptr::null_mut(),
                    max_splice_size,
                    libc::SPLICE_F_MOVE | libc::SPLICE_F_MORE | libc::SPLICE_F_NONBLOCK
                )
            };
            
            #[cfg(not(target_os = "linux"))]
            let splice_result = {
                // Linux가 아닌 시스템에서는 일반 read/write로 대체
                let mut buffer = vec![0u8; max_splice_size];
                match nix::unistd::read(client_fd, &mut buffer) {
                    Ok(n) if n > 0 => {
                        buffer.truncate(n);
                        match nix::unistd::write(pipe_c2s_write, &buffer) {
                            Ok(m) => m as isize,
                            Err(_) => -1
                        }
                    },
                    Ok(_) => 0,
                    Err(_) => -1
                }
            };
            
            match splice_result {
                0 => break, // 연결 종료
                n if n > 0 => {
                    let n = n as usize;
                    
                    // 동적으로 최적의 스플라이스 크기 조정
                    if n == max_splice_size && max_splice_size < BUFFER_SIZE_LARGE {
                        max_splice_size = std::cmp::min(max_splice_size * 2, BUFFER_SIZE_LARGE);
                    }
                    
                    // 파이프에서 서버로 데이터 전송
                    #[cfg(target_os = "linux")]
                    let splice_out_result = unsafe {
                        libc::splice(
                            pipe_c2s_read,
                            std::ptr::null_mut(),
                            server_fd,
                            std::ptr::null_mut(),
                            n,
                            libc::SPLICE_F_MOVE | libc::SPLICE_F_MORE | libc::SPLICE_F_NONBLOCK
                        )
                    };
                    
                    #[cfg(not(target_os = "linux"))]
                    let splice_out_result = {
                        let mut buffer = vec![0u8; n];
                        match nix::unistd::read(pipe_c2s_read, &mut buffer) {
                            Ok(m) if m > 0 => {
                                buffer.truncate(m);
                                match nix::unistd::write(server_fd, &buffer) {
                                    Ok(m) => m as isize,
                                    Err(_) => -1
                                }
                            },
                            Ok(_) => 0,
                            Err(_) => -1
                        }
                    };
                    
                    match splice_out_result {
                        m if m as usize == n => {
                            total_bytes += n as u64;
                            metrics_clone.add_http_bytes_in(n as u64);
                            
                            // 주기적으로 로그 출력 (1MB 마다)
                            if total_bytes % (1024 * 1024) < (n as u64) {
                                debug!("[Session:{}] HTTP 클라이언트→서버 누적(splice): {} KB", 
                                       session_id_clone, total_bytes / 1024);
                            }
                        },
                        m if m > 0 => {
                            let m = m as usize;
                            warn!("[Session:{}] Partial splice to server: {} of {} bytes", 
                                  session_id_clone, m, n);
                            
                            // 남은 데이터 처리 시도
                            let mut bytes_left = n - m;
                            total_bytes += m as u64;
                            metrics_clone.add_http_bytes_in(m as u64);
                            
                            while bytes_left > 0 {
                                #[cfg(target_os = "linux")]
                                let retry_result = unsafe {
                                    libc::splice(
                                        pipe_c2s_read,
                                        std::ptr::null_mut(),
                                        server_fd,
                                        std::ptr::null_mut(),
                                        bytes_left,
                                        libc::SPLICE_F_MOVE | libc::SPLICE_F_MORE | libc::SPLICE_F_NONBLOCK
                                    )
                                };
                                
                                #[cfg(not(target_os = "linux"))]
                                let retry_result = {
                                    let mut buffer = vec![0u8; bytes_left];
                                    match nix::unistd::read(pipe_c2s_read, &mut buffer) {
                                        Ok(m) if m > 0 => {
                                            buffer.truncate(m);
                                            match nix::unistd::write(server_fd, &buffer) {
                                                Ok(m) => m as isize,
                                                Err(_) => -1
                                            }
                                        },
                                        Ok(_) => 0,
                                        Err(_) => -1
                                    }
                                };
                                
                                match retry_result {
                                    0 => break,
                                    b if b > 0 => {
                                        let b = b as usize;
                                        bytes_left -= b;
                                        total_bytes += b as u64;
                                        metrics_clone.add_http_bytes_in(b as u64);
                                    },
                                    -1 => {
                                        if is_would_block_error(std::io::Error::last_os_error()) {
                                            // 비동기 작업이므로 짧은 대기 후 재시도
                                            std::thread::sleep(std::time::Duration::from_millis(1));
                                            continue;
                                        }
                                        error!("[Session:{}] Failed to splice remaining data to server", 
                                               session_id_clone);
                                        break;
                                    },
                                    _ => {
                                        error!("[Session:{}] Unexpected splice result", session_id_clone);
                                        break;
                                    }
                                }
                            }
                        },
                        -1 => {
                            if is_would_block_error(std::io::Error::last_os_error()) {
                                // 비동기 작업이므로 짧은 대기 후 재시도
                                std::thread::sleep(std::time::Duration::from_millis(1));
                                continue;
                            }
                            error!("[Session:{}] Failed to splice to server", session_id_clone);
                            break;
                        },
                        _ => {
                            error!("[Session:{}] Unexpected splice result", session_id_clone);
                            break;
                        }
                    }
                },
                -1 => {
                    if is_would_block_error(std::io::Error::last_os_error()) {
                        // 비동기 작업이므로 짧은 대기 후 재시도
                        std::thread::sleep(std::time::Duration::from_millis(1));
                        continue;
                    }
                    error!("[Session:{}] Failed to splice from client", session_id_clone);
                    break;
                },
                _ => {
                    error!("[Session:{}] Unexpected splice result", session_id_clone);
                    break;
                }
            }
        }
        
        debug!("[Session:{}] Client to server splice transfer finished: {} bytes", 
               session_id_clone, total_bytes);
        
        // 파이프 정리
        let _ = close(pipe_c2s_read);
        let _ = close(pipe_c2s_write);
        
        total_bytes
    });
    
    let metrics_clone = Arc::clone(&metrics);
    let session_id_clone = session_id.to_string();
    
    let server_to_client_handle = std::thread::spawn(move || {
        let mut total_bytes = 0u64;
        let mut max_splice_size = BUFFER_SIZE_SMALL; // 초기 크기
        
        loop {
            // 서버로부터 파이프로 데이터 전송
            #[cfg(target_os = "linux")]
            let splice_result = unsafe {
                libc::splice(
                    server_fd,
                    std::ptr::null_mut(),
                    pipe_s2c_write,
                    std::ptr::null_mut(),
                    max_splice_size,
                    libc::SPLICE_F_MOVE | libc::SPLICE_F_MORE | libc::SPLICE_F_NONBLOCK
                )
            };
            
            #[cfg(not(target_os = "linux"))]
            let splice_result = {
                // Linux가 아닌 시스템에서는 일반 read/write로 대체
                let mut buffer = vec![0u8; max_splice_size];
                match nix::unistd::read(server_fd, &mut buffer) {
                    Ok(n) if n > 0 => {
                        buffer.truncate(n);
                        match nix::unistd::write(pipe_s2c_write, &buffer) {
                            Ok(m) => m as isize,
                            Err(_) => -1
                        }
                    },
                    Ok(_) => 0,
                    Err(_) => -1
                }
            };
            
            match splice_result {
                0 => break, // 연결 종료
                n if n > 0 => {
                    let n = n as usize;
                    
                    // 동적으로 최적의 스플라이스 크기 조정
                    if n == max_splice_size && max_splice_size < BUFFER_SIZE_LARGE {
                        max_splice_size = std::cmp::min(max_splice_size * 2, BUFFER_SIZE_LARGE);
                    }
                    
                    // 파이프에서 클라이언트로 데이터 전송
                    #[cfg(target_os = "linux")]
                    let splice_out_result = unsafe {
                        libc::splice(
                            pipe_s2c_read,
                            std::ptr::null_mut(),
                            client_fd,
                            std::ptr::null_mut(),
                            n,
                            libc::SPLICE_F_MOVE | libc::SPLICE_F_MORE | libc::SPLICE_F_NONBLOCK
                        )
                    };
                    
                    #[cfg(not(target_os = "linux"))]
                    let splice_out_result = {
                        let mut buffer = vec![0u8; n];
                        match nix::unistd::read(pipe_s2c_read, &mut buffer) {
                            Ok(m) if m > 0 => {
                                buffer.truncate(m);
                                match nix::unistd::write(client_fd, &buffer) {
                                    Ok(m) => m as isize,
                                    Err(_) => -1
                                }
                            },
                            Ok(_) => 0,
                            Err(_) => -1
                        }
                    };
                    
                    match splice_out_result {
                        m if m as usize == n => {
                            total_bytes += n as u64;
                            metrics_clone.add_http_bytes_out(n as u64);
                        },
                        m if m > 0 => {
                            let m = m as usize;
                            warn!("[Session:{}] Partial splice to client: {} of {} bytes", 
                                  session_id_clone, m, n);
                            
                            // 남은 데이터 처리 시도
                            let mut bytes_left = n - m;
                            total_bytes += m as u64;
                            metrics_clone.add_http_bytes_out(m as u64);
                            
                            while bytes_left > 0 {
                                #[cfg(target_os = "linux")]
                                let retry_result = unsafe {
                                    libc::splice(
                                        pipe_s2c_read,
                                        std::ptr::null_mut(),
                                        client_fd,
                                        std::ptr::null_mut(),
                                        bytes_left,
                                        libc::SPLICE_F_MOVE | libc::SPLICE_F_MORE | libc::SPLICE_F_NONBLOCK
                                    )
                                };
                                
                                #[cfg(not(target_os = "linux"))]
                                let retry_result = {
                                    let mut buffer = vec![0u8; bytes_left];
                                    match nix::unistd::read(pipe_s2c_read, &mut buffer) {
                                        Ok(m) if m > 0 => {
                                            buffer.truncate(m);
                                            match nix::unistd::write(client_fd, &buffer) {
                                                Ok(m) => m as isize,
                                                Err(_) => -1
                                            }
                                        },
                                        Ok(_) => 0,
                                        Err(_) => -1
                                    }
                                };
                                
                                match retry_result {
                                    0 => break,
                                    b if b > 0 => {
                                        let b = b as usize;
                                        bytes_left -= b;
                                        total_bytes += b as u64;
                                        metrics_clone.add_http_bytes_out(b as u64);
                                    },
                                    -1 => {
                                        if is_would_block_error(std::io::Error::last_os_error()) {
                                            // 비동기 작업이므로 짧은 대기 후 재시도
                                            std::thread::sleep(std::time::Duration::from_millis(1));
                                            continue;
                                        }
                                        error!("[Session:{}] Failed to splice remaining data to client", 
                                               session_id_clone);
                                        break;
                                    },
                                    _ => {
                                        error!("[Session:{}] Unexpected splice result", session_id_clone);
                                        break;
                                    }
                                }
                            }
                        },
                        -1 => {
                            if is_would_block_error(std::io::Error::last_os_error()) {
                                // 비동기 작업이므로 짧은 대기 후 재시도
                                std::thread::sleep(std::time::Duration::from_millis(1));
                                continue;
                            }
                            error!("[Session:{}] Failed to splice to client", session_id_clone);
                            break;
                        },
                        _ => {
                            error!("[Session:{}] Unexpected splice result", session_id_clone);
                            break;
                        }
                    }
                },
                -1 => {
                    if is_would_block_error(std::io::Error::last_os_error()) {
                        // 비동기 작업이므로 짧은 대기 후 재시도
                        std::thread::sleep(std::time::Duration::from_millis(1));
                        continue;
                    }
                    error!("[Session:{}] Failed to splice from server", session_id_clone);
                    break;
                },
                _ => {
                    error!("[Session:{}] Unexpected splice result", session_id_clone);
                    break;
                }
            }
        }
        
        debug!("[Session:{}] Server to client splice transfer finished: {} bytes", 
               session_id_clone, total_bytes);
        
        // 파이프 정리
        let _ = close(pipe_s2c_read);
        let _ = close(pipe_s2c_write);
        
        total_bytes
    });
    
    // 두 스레드가 종료될 때까지 대기
    let c2s_bytes = client_to_server_handle.join().unwrap_or(0);
    let s2c_bytes = server_to_client_handle.join().unwrap_or(0);
    
    // 세션 전체 시간 기록
    let total_session_time = request_start_time.elapsed().as_millis() as u64;
    info!(
        "[Session:{}] HTTP 세션 종료(splice 사용), 총 세션 시간: {} ms, 클라이언트→서버: {} bytes, 서버→클라이언트: {} bytes", 
        session_id, total_session_time, c2s_bytes, s2c_bytes
    );
    
    Ok(())
}

/// 비동기적인 파이프 생성 (O_NONBLOCK 플래그 사용)
fn create_nonblocking_pipe() -> Result<(RawFd, RawFd), io::Error> {
    // 파이프 생성
    let (read_fd, write_fd) = pipe().map_err(|e| {
        io::Error::new(io::ErrorKind::Other, format!("Failed to create pipe: {}", e))
    })?;
    
    // 읽기 파이프를 비동기적으로 설정 (libc 직접 사용)
    let read_fd_raw = read_fd.as_raw_fd();
    unsafe {
        let flags = libc::fcntl(read_fd_raw, libc::F_GETFL);
        if flags < 0 {
            let _ = close(read_fd);
            let _ = close(write_fd);
            return Err(io::Error::last_os_error());
        }
        
        if libc::fcntl(read_fd_raw, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
            let _ = close(read_fd);
            let _ = close(write_fd);
            return Err(io::Error::last_os_error());
        }
    }
    
    // 쓰기 파이프를 비동기적으로 설정 (libc 직접 사용)
    let write_fd_raw = write_fd.as_raw_fd();
    unsafe {
        let flags = libc::fcntl(write_fd_raw, libc::F_GETFL);
        if flags < 0 {
            let _ = close(read_fd);
            let _ = close(write_fd);
            return Err(io::Error::last_os_error());
        }
        
        if libc::fcntl(write_fd_raw, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
            let _ = close(read_fd);
            let _ = close(write_fd);
            return Err(io::Error::last_os_error());
        }
    }
    
    // 성공 시 raw fd 반환
    Ok((read_fd_raw, write_fd_raw))
}

/// TCP 소켓 최적화 설정
fn set_tcp_socket_options(
    client_stream: &TcpStream, 
    server_stream: &TcpStream
) -> Result<(), io::Error> {
    // TCP_NODELAY 설정
    if TCP_NODELAY {
        client_stream.set_nodelay(true)?;
        server_stream.set_nodelay(true)?;
    }
    
    // Linux 전용 TCP 최적화
    #[cfg(target_os = "linux")]
    {
        if TCP_QUICKACK {
            // 클라이언트 퀵ACK 설정
            let optval: libc::c_int = 1;
            unsafe {
                if libc::setsockopt(
                    client_stream.as_raw_fd(),
                    libc::IPPROTO_TCP,
                    libc::TCP_QUICKACK,
                    &optval as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&optval) as libc::socklen_t,
                ) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                
                // 서버 퀵ACK 설정
                if libc::setsockopt(
                    server_stream.as_raw_fd(),
                    libc::IPPROTO_TCP,
                    libc::TCP_QUICKACK,
                    &optval as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&optval) as libc::socklen_t,
                ) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
        }
    }
    
    Ok(())
}

/// 소켓 버퍼 크기를 최적화하여 데이터 처리량 향상
fn optimize_socket_buffers(client_fd: RawFd, server_fd: RawFd) -> Result<(), io::Error> {
    // Linux 전용 소켓 버퍼 최적화
    #[cfg(target_os = "linux")]
    {
        let buf_size = BUFFER_SIZE_LARGE as libc::c_int;
        
        // 클라이언트 소켓 버퍼 크기 최적화
        unsafe {
            // 수신 버퍼 크기 설정
            if libc::setsockopt(
                client_fd,
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &buf_size as *const _ as *const libc::c_void,
                std::mem::size_of_val(&buf_size) as libc::socklen_t,
            ) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            
            // 송신 버퍼 크기 설정
            if libc::setsockopt(
                client_fd,
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                &buf_size as *const _ as *const libc::c_void,
                std::mem::size_of_val(&buf_size) as libc::socklen_t,
            ) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            
            // 서버 소켓 버퍼 크기 최적화
            // 수신 버퍼 크기 설정
            if libc::setsockopt(
                server_fd,
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &buf_size as *const _ as *const libc::c_void,
                std::mem::size_of_val(&buf_size) as libc::socklen_t,
            ) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            
            // 송신 버퍼 크기 설정
            if libc::setsockopt(
                server_fd,
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                &buf_size as *const _ as *const libc::c_void,
                std::mem::size_of_val(&buf_size) as libc::socklen_t,
            ) < 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
    }
    
    Ok(())
}

/// EAGAIN/EWOULDBLOCK 오류인지 확인
fn is_would_block_error(err: io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock
} 