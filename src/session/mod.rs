use std::error::Error;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, FromRawFd};
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::{debug, error, info};
use socket2::Socket;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use bytes::BytesMut;

use crate::config::Config;
use crate::constants::*;
use crate::metrics::Metrics;
use crate::buffer::BufferPool;
use crate::tls::{accept_tls_with_cert, connect_tls, generate_fake_cert};
use crate::proxy::http::proxy_http_streams;
use crate::proxy::tls::proxy_tls_streams;
use crate::acl::domain_blocker::DomainBlocker;
use crate::acl::block_page::BlockPage;
use crate::REQUEST_LOGGER;

/// HTTP 요청 파싱 결과
#[derive(Debug)]
struct HttpRequest {
    method: String,
    host: String,
    port: u16,
    path: String,
    headers: String,
    body: Option<String>,
}

pub struct Session {
    client_stream: Option<TcpStream>,
    client_addr: SocketAddr,
    metrics: Arc<Metrics>,
    config: Arc<Config>,
    buffer_pool: Option<Arc<BufferPool>>,
    start_time: Instant,
    domain_blocker: Arc<DomainBlocker>,
    block_page: BlockPage,
}

impl Session {
    pub fn new(client_stream: TcpStream, client_addr: SocketAddr, metrics: Arc<Metrics>, config: Arc<Config>, buffer_pool: Option<Arc<BufferPool>>) -> Self {
        Self {
            client_stream: Some(client_stream),
            client_addr,
            metrics: Arc::clone(&metrics),
            config: Arc::clone(&config),
            buffer_pool,
            start_time: Instant::now(),
            domain_blocker: Arc::new(DomainBlocker::new(Arc::clone(&config))),
            block_page: BlockPage::new(),
        }
    }

    pub async fn handle(mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("[Session:{}] session start, addr: {}", self.session_id(), self.client_addr);

        let mut client_stream = match self.client_stream.take() {
            Some(stream) => stream,
            None => return Ok(())
        };

        debug!("[Session:{}] TCP 소켓 최적화 시작", self.session_id());
        self.optimize_tcp(&client_stream)?;
        debug!("[Session:{}] TCP 소켓 최적화 완료", self.session_id());

        // 버퍼 할당
        let mut buffer = self.allocate_buffer();

        // 클라이언트 요청 읽기
        let n = match self.read_client_request(&mut client_stream, &mut buffer).await {
            Ok(0) => {
                debug!("[Session:{}] Received an empty request, connection closed.", self.session_id());
                if let Some(pool) = self.buffer_pool {
                    pool.return_buffer(buffer);
                }
                return Ok(());
            },
            Ok(n) => n,
            Err(e) => {
                if let Some(pool) = self.buffer_pool {
                    pool.return_buffer(buffer);
                }
                return Err(e);
            }
        };

        // 요청 파싱
        let request_data = &buffer[0..n];
        let request_str = String::from_utf8_lossy(request_data).to_string();
        
        // HTTP 요청 파싱
        let http_request = match self.parse_http_request(&request_str) {
            Ok(req) => req,
            Err(e) => {
                error!("[Session:{}] Failed to parse HTTP request: {}", self.session_id(), e);
                if let Some(pool) = self.buffer_pool {
                    pool.return_buffer(buffer);
                }
                return Err(e);
            }
        };
        
        let is_connect = http_request.method == "CONNECT";
        let host = &http_request.host;
        let port = http_request.port;

        // 도메인 차단 확인
        if self.domain_blocker.is_blocked(host) {
            return self.handle_blocked_domain(client_stream, host, is_connect, &request_str, buffer).await;
        }

        // 요청 처리
        self.metrics.increment_request_count(is_connect);

        if is_connect {
            // HTTPS 요청 처리
            self.handle_https_request(client_stream, host, buffer).await
        } else {
            // HTTP 요청 처리
            self.handle_http_request(client_stream, host, port, &request_str, n, buffer).await
        }
    }
    
    /// 버퍼 할당
    fn allocate_buffer(&self) -> BytesMut {
        if let Some(pool) = &self.buffer_pool {
            debug!("[Session:{}] 버퍼 풀에서 버퍼 할당 (크기: {})", self.session_id(), self.config.buffer_size);
            pool.get_buffer(Some(self.config.buffer_size))
        } else {
            debug!("[Session:{}] 새 버퍼 생성 (크기: {})", self.session_id(), self.config.buffer_size);
            BytesMut::with_capacity(self.config.buffer_size)
        }
    }
    
    /// 클라이언트 요청 읽기
    async fn read_client_request(&self, client_stream: &mut TcpStream, buffer: &mut BytesMut) -> Result<usize, Box<dyn Error + Send + Sync>> {
        match tokio::time::timeout(
            Duration::from_millis(self.config.timeout_ms as u64),
            client_stream.read_buf(buffer),
        ).await {
            Ok(Ok(n)) => Ok(n),
            Ok(Err(e)) => {
                error!("[Session:{}] read client request failed: {}", self.session_id(), e);
                Err(Box::new(e))
            },
            Err(_) => {
                error!("[Session:{}] client request read timed out", self.session_id());
                Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "client request read timed out"
                )))
            }
        }
    }
    
    /// HTTP 요청 파싱
    fn parse_http_request(&self, request_str: &str) -> Result<HttpRequest, Box<dyn Error + Send + Sync>> {
        let first_line = request_str.lines().next().unwrap_or("");
        debug!("[Session:{}] request first line: {}", self.session_id(), first_line);

        let parts: Vec<&str> = first_line.split_whitespace().collect();
        let method = parts.get(0).unwrap_or(&"").to_string();
        let is_connect = method == "CONNECT";

        if is_connect {
            // CONNECT 메서드 파싱
            if let Some(host_port) = parts.get(1) {
                if let Some(idx) = host_port.rfind(':') {
                    let host = &host_port[0..idx];
                    let port = host_port[idx+1..].parse::<u16>()
                        .map_err(|_| {
                            error!("[Session:{}] Invalid port in CONNECT request", self.session_id());
                            std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid port")
                        })?;
                    
                    Ok(HttpRequest {
                        method,
                        host: host.to_string(),
                        port,
                        path: "/".to_string(),
                        headers: "".to_string(),
                        body: None,
                    })
                } else {
                    Ok(HttpRequest {
                        method,
                        host: host_port.to_string(),
                        port: 443,
                        path: "/".to_string(),
                        headers: "".to_string(),
                        body: None,
                    })
                }
            } else {
                error!("[Session:{}] Missing host in CONNECT request", self.session_id());
                Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Missing host").into())
            }
        } else {
            // HTTP 메서드 파싱
            let mut host = String::new();
            let mut port = 80;
            let mut path = "/".to_string();
            
            // Host 헤더에서 호스트 추출
            if let Some(host_line) = request_str.lines()
                .find(|line| line.to_lowercase().starts_with("host:")) {
                let host_value = host_line.trim_start_matches("Host:").trim_start_matches("host:").trim();

                // 포트 확인
                if let Some(idx) = host_value.rfind(':') {
                    host = host_value[0..idx].to_string();
                    if let Ok(p) = host_value[idx+1..].parse::<u16>() {
                        port = p;
                    }
                } else {
                    host = host_value.to_string();
                }
            } else if let Some(url_part) = parts.get(1) {
                // URL에서 호스트 추출
                if url_part.starts_with("http://") {
                    let without_scheme = url_part.trim_start_matches("http://");
                    if let Some(host_part) = without_scheme.split('/').next() {
                        if !host_part.is_empty() {
                            // 포트 확인
                            if let Some(idx) = host_part.rfind(':') {
                                host = host_part[0..idx].to_string();
                                if let Ok(p) = host_part[idx+1..].parse::<u16>() {
                                    port = p;
                                }
                            } else {
                                host = host_part.to_string();
                            }
                            
                            // 경로 추출
                            if let Some(path_idx) = without_scheme.find('/') {
                                path = without_scheme[path_idx..].to_string();
                                if path.is_empty() {
                                    path = "/".to_string();
                                }
                            }
                        } else {
                            error!("[Session:{}] Invalid host in URL", self.session_id());
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid host in URL").into());
                        }
                    } else {
                        error!("[Session:{}] Invalid URL format", self.session_id());
                        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid URL format").into());
                    }
                } else {
                    // URL에서 경로만 추출
                    path = url_part.to_string();
                }
            }
            
            if host.is_empty() {
                error!("[Session:{}] Missing host header and invalid URL", self.session_id());
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Missing host information").into());
            }
            
            // 헤더와 본문 분리
            let mut headers = String::new();
            let mut body = None;
            
            if let Some(header_end) = request_str.find("\r\n\r\n") {
                headers = request_str[..header_end].to_string();
                
                // 본문 추출 (있는 경우)
                let body_start = header_end + 4; // "\r\n\r\n" 길이
                if body_start < request_str.len() {
                    body = Some(request_str[body_start..].to_string());
                }
            } else {
                headers = request_str.to_string();
            }
            
            Ok(HttpRequest {
                method,
                host,
                port,
                path,
                headers,
                body,
            })
        }
    }
    
    /// 차단된 도메인 처리
    async fn handle_blocked_domain(&self, mut client_stream: TcpStream, host: &str, is_connect: bool, request_str: &str, buffer: BytesMut) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("[Session:{}] Blocked access to domain: {}", self.session_id(), host);
        
        // 클라이언트 IP 주소 가져오기
        let client_ip = self.client_addr.ip().to_string();
        
        if !is_connect {
            // HTTP 차단 페이지 전송
            if let Err(e) = self.block_page.send_http_block_page(
                &mut client_stream, 
                host, 
                &self.session_id(),
                Some(request_str),
                Some(&client_ip)
            ).await {
                error!("[Session:{}] Failed to send HTTP block page: {}", self.session_id(), e);
            }
            
            // 로그 저장 - 차단된 HTTP 요청
            self.log_blocked_request(host, request_str, &client_ip, false).await;
            
        } else {
            // HTTPS 차단 페이지 전송
            if let Err(e) = self.block_page.handle_https_block(
                client_stream, 
                host, 
                &self.session_id(),
                Some(request_str),
                Some(&client_ip)
            ).await {
                error!("[Session:{}] Failed to handle HTTPS block: {}", self.session_id(), e);
            }
            
            // 로그 저장 - 차단된 HTTPS 요청
            self.log_blocked_request(host, request_str, &client_ip, true).await;
            
            // 연결 종료 시 활성 연결 카운트 감소
            self.metrics.connection_closed(true);
        }
        
        if let Some(pool) = &self.buffer_pool {
            pool.return_buffer(buffer);
        }
        
        Ok(())
    }
    
    /// 차단된 요청 로깅
    async fn log_blocked_request(&self, host: &str, request_str: &str, client_ip: &str, is_tls: bool) {
        if let Ok(logger) = REQUEST_LOGGER.try_read() {
            // 요청 파싱
            let mut method = "UNKNOWN".to_string();
            let mut path = "/".to_string();
            let headers;
            let mut body = None;
            
            // 요청 라인 추출 및 메서드, 경로 파싱
            if let Some(first_line) = request_str.lines().next() {
                let parts: Vec<&str> = first_line.split_whitespace().collect();
                if parts.len() >= 2 {
                    method = parts[0].to_string();
                    path = parts[1].to_string();
                }
            }
            
            // 헤더와 본문 분리
            if let Some(header_end) = request_str.find("\r\n\r\n") {
                headers = request_str[..header_end].to_string();
                
                // 본문 추출 (있는 경우)
                let body_start = header_end + 4; // "\r\n\r\n" 길이
                if body_start < request_str.len() {
                    body = Some(request_str[body_start..].to_string());
                }
            } else {
                headers = request_str.to_string();
            }
            
            // 로그 저장
            if let Err(e) = logger.log_async(
                host.to_string(),
                &method,
                &path,
                &headers,
                body,
                &self.session_id(),
                client_ip,
                "Blocked", // 차단된 요청은 타겟 IP를 "Blocked"로 표시
                None, // 응답 시간 없음
                true,  // 차단된 요청
                is_tls  // TLS 여부
            ) {
                debug!("[Session:{}] 차단된 요청 로깅 실패: {}", self.session_id(), e);
            }
        }
    }
    
    /// HTTPS 요청 처리
    async fn handle_https_request(&self, mut client_stream: TcpStream, host: &str, buffer: BytesMut) -> Result<(), Box<dyn Error + Send + Sync>> {
        // CONNECT 요청에 대한 승인 응답 전송
        let response = "HTTP/1.1 200 Connection Established\r\nConnection: keep-alive\r\n\r\n";
        client_stream.write_all(response.as_bytes()).await?;
        info!("[Session:{}] CONNECT request approved for {}", self.session_id(), host);
        
        // 버퍼 반환
        if let Some(pool) = &self.buffer_pool {
            pool.return_buffer(buffer);
        }
        
        // 1. 가짜 인증서로 클라이언트와 TLS 연결
        let fake_cert = generate_fake_cert(host).await?;
        info!("[Session:{}] Generated fake certificate for {}", self.session_id(), host);
        
        let tls_stream = accept_tls_with_cert(client_stream, fake_cert).await?;
        info!("[Session:{}] Established TLS with client for {}", self.session_id(), host);
        
        // 2. 실제 서버와도 TLS 연결 - 설정 전달하여 인증서 검증 옵션 적용
        match connect_tls(host, &self.config).await {
            Ok(real_tls_stream) => {
                info!("[Session:{}] Connected to real server {} over TLS", self.session_id(), host);
                
                // 3. 중간에서 데이터 가로채기 - 요청 시작 시간 전달
                let request_start_time = Instant::now();
                proxy_tls_streams(tls_stream, real_tls_stream, Arc::clone(&self.metrics), &self.session_id(), host, request_start_time).await?;
                
                // 연결 종료 시 활성 연결 카운트 감소
                self.metrics.connection_closed(true);
                info!("[Session:{}] Completed TLS proxy for {}", self.session_id(), host);
                Ok(())
            },
            Err(e) => {
                // UnknownIssuer 오류 발생 시 설정 안내
                if e.to_string().contains("UnknownIssuer") {
                    error!("[Session:{}] 서버 인증서 검증 실패: {}", self.session_id(), e);
                }
                Err(e)
            }
        }
    }
    
    /// HTTP 요청 처리
    async fn handle_http_request(&self, client_stream: TcpStream, host: &str, port: u16, request_str: &str, n: usize, buffer: BytesMut) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("[Session:{}] Processing HTTP request for {}", self.session_id(), host);
        
        // 서버에 연결
        let server_addr = format!("{}:{}", host, port);
        let server_stream = match TcpStream::connect(&server_addr).await {
            Ok(stream) => {
                // 실제 연결된 IP 주소 확인 및 로깅
                let target_ip = if let Ok(peer_addr) = stream.peer_addr() {
                    info!("[Session:{}] Connected to IP: {} for host: {}", self.session_id(), peer_addr.ip(), host);
                    peer_addr.ip().to_string()
                } else {
                    "Unknown IP".to_string()
                };
                
                // 첫 번째 HTTP 요청 로깅
                self.log_http_request(host, request_str, &target_ip).await;
                
                stream
            },
            Err(e) => {
                error!("[Session:{}] Failed to connect to target server {}: {}", self.session_id(), server_addr, e);
                if let Some(pool) = &self.buffer_pool {
                    pool.return_buffer(buffer);
                }
                return Err(e.into());
            }
        };
        
        // 서버에 요청 전달
        let buffer_slice = &buffer[0..n];
        if let Err(e) = server_stream.try_write(buffer_slice) {
            error!("[Session:{}] Failed to forward request to server: {}", self.session_id(), e);
            if let Some(pool) = &self.buffer_pool {
                pool.return_buffer(buffer);
            }
            return Err(e.into());
        }
        
        // 버퍼 반환
        if let Some(pool) = &self.buffer_pool {
            pool.return_buffer(buffer);
        }
        
        // 프록시 시작 - 요청 시작 시간 전달
        info!("[Session:{}] Starting HTTP proxy for {}", self.session_id(), host);
        let request_start_time = Instant::now();
        proxy_http_streams(client_stream, server_stream, Arc::clone(&self.metrics), &self.session_id(), request_start_time).await?;
        
        // 연결 종료 시 활성 연결 카운트 감소
        self.metrics.connection_closed(false);
        info!("[Session:{}] Completed HTTP proxy for {}", self.session_id(), host);
        
        Ok(())
    }
    
    /// HTTP 요청 로깅
    async fn log_http_request(&self, host: &str, request_str: &str, target_ip: &str) {
        if let Ok(logger) = REQUEST_LOGGER.try_read() {
            // 요청 파싱
            let mut method = "UNKNOWN".to_string();
            let mut path = "/".to_string();
            let headers;
            let mut body = None;
            
            // 요청 라인 추출 및 메서드, 경로 파싱
            if let Some(first_line) = request_str.lines().next() {
                let parts: Vec<&str> = first_line.split_whitespace().collect();
                if parts.len() >= 2 {
                    method = parts[0].to_string();
                    path = parts[1].to_string();
                }
            }
            
            // 헤더와 본문 분리
            if let Some(header_end) = request_str.find("\r\n\r\n") {
                headers = request_str[..header_end].to_string();
                
                // 본문 추출 (있는 경우)
                let body_start = header_end + 4; // "\r\n\r\n" 길이
                if body_start < request_str.len() {
                    body = Some(request_str[body_start..].to_string());
                }
            } else {
                headers = request_str.to_string();
            }
            
            // 클라이언트 IP 주소 가져오기
            let client_ip = self.client_addr.ip().to_string();
            
            // 로그 저장
            match logger.log_async(
                host.to_string(),
                &method,
                &path,
                &headers,
                body,
                &self.session_id(),
                &client_ip,
                target_ip,
                None,  // 응답 시간은 아직 알 수 없음
                false,   // 차단되지 않은 요청
                false    // TLS 아님
            ) {
                Ok(_) => debug!("[Session:{}] HTTP 요청 로깅 성공", self.session_id()),
                Err(e) => debug!("[Session:{}] HTTP 요청 로깅 실패: {}", self.session_id(), e)
            }
        } else {
            debug!("[Session:{}] RequestLogger 인스턴스에 접근할 수 없습니다", self.session_id());
        }
    }

    // 세션 ID 생성
    fn session_id(&self) -> String {
        format!("{:x}", self.start_time.elapsed().as_nanos() & 0xFFFFFF)
    }

    // tcp 최적화
    fn optimize_tcp(&self, stream: &TcpStream) -> Result<(), Box<dyn Error + Send + Sync>> {
        stream.set_nodelay(TCP_NODELAY)?;

        let fd = stream.as_raw_fd();
        let sock = unsafe { Socket::from_raw_fd(fd) };

        let _ = sock.set_recv_buffer_size(BUFFER_SIZE_MEDIUM)
            .map_err(|e| debug!("[Session:{}] set receive buffer size failed: {}", self.session_id(), e));
        let _ = sock.set_send_buffer_size(BUFFER_SIZE_MEDIUM)
            .map_err(|e| debug!("[Session:{}] set send buffer size failed: {}", self.session_id(), e));

        let _ = sock.set_keepalive(true)
            .map_err(|e| debug!("[Session:{}] TCP Keepalive activate failed: {}", self.session_id(), e));

        std::mem::forget(sock);

        Ok(())
    }
}