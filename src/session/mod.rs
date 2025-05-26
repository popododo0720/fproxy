use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::error::Error;
use std::os::unix::io::{AsRawFd};
use std::os::unix::io::FromRawFd;

use bytes::BytesMut;
use log::{trace, debug, info, error, warn};
use tokio::net::TcpStream;
use socket2::{Socket};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use chrono;

use crate::metrics::Metrics;
use crate::config::Config;
use crate::buffer::BufferPool;
use crate::constants::*;
use crate::tls::{generate_fake_cert, accept_tls_with_cert, connect_tls};
use crate::proxy::{proxy_http_streams, proxy_tls_streams};
use crate::acl::domain_blocker::DomainBlocker;


pub struct Session {
    client_stream: Option<TcpStream>,
    client_addr: SocketAddr,
    metrics: Arc<Metrics>,
    config: Arc<Config>,
    buffer_pool: Option<Arc<BufferPool>>,
    start_time: Instant,
    domain_blocker: Arc<DomainBlocker>,
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
        }
    }

    pub async fn handle(mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        trace!("[Session:{}] session start, addr: {}", self.session_id(), self.client_addr);

        let mut client_stream = match self.client_stream.take() {
            Some(stream) => stream,
            None => return Ok(())
        };

        self.optimize_tcp(&client_stream)?;

        let mut buffer = if let Some(pool) = &self.buffer_pool {
            pool.get_buffer(Some(self.config.buffer_size))
        } else {
            BytesMut::with_capacity(self.config.buffer_size)
        };

        let n = match tokio::time::timeout(
            Duration::from_millis(self.config.timeout_ms as u64),
            client_stream.read_buf(&mut buffer),
        ).await {
            Ok(Ok(n)) => n,
            Ok(Err(e)) => {
                error!("[Session:{}] read client request failed: {}", self.session_id(), e);
                return Err(Box::new(e));
            },
            Err(_) => {
                error!("[Session:{}] client request read timed out", self.session_id());
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "client request read timed out"
                )));
            }
        };

        if n == 0 {
            debug!("[Session:{}] Received an empty request, connection closed.", self.session_id());
            if let Some(pool) = self.buffer_pool {
                pool.return_buffer(buffer);
            }

            return Ok(());
        }

        let request_data = &buffer[0..n];
        let request_str = String::from_utf8_lossy(request_data);

        let first_line = request_str.lines().next().unwrap_or("");
        trace!("[Session:{}] request first line: {}", self.session_id(), first_line);

        let parts: Vec<&str> = first_line.split_whitespace().collect();
        let method = parts.get(0).unwrap_or(&"");
        let is_connect = *method == "CONNECT";

        let (host, port) = if is_connect {
            if let Some(host_port) = parts.get(1) {
                if let Some(idx) = host_port.rfind(':') {
                    let host = &host_port[0..idx];
                    let port = host_port[idx+1..].parse::<u16>()
                        .map_err(|_| {
                            error!("[Session:{}] Invalid port in CONNECT request", self.session_id());
                            std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid port")
                        })?;
                    (host.to_string(), port)
                } else {
                    (host_port.to_string(), 443)
                }
            } else {
                error!("[Session:{}] Missing host in CONNECT request", self.session_id());
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Missing host").into());
            }
        } else {
            // HTTP - host header
            if let Some(host_line) = request_str.lines()
                .find(|line| line.to_lowercase().starts_with("host:")) {
                let host_value = host_line.trim_start_matches("Host:").trim_start_matches("host:").trim();

                // check if the host includes a port
                let host = if let Some(idx) = host_value.rfind(':') {
                    &host_value[0..idx]
                } else {
                    host_value
                };
                
                (host.to_string(), 80) // 기본 HTTP 포트
            } else if let Some(url_part) = parts.get(1) {
                // extract host from url
                if url_part.starts_with("http://") {
                    let without_scheme = url_part.trim_start_matches("http://");
                    if let Some(host) = without_scheme.split('/').next() {
                        if !host.is_empty() {
                            (host.trim().to_string(), 80)
                        } else {
                            error!("[Session:{}] Invalid host in URL", self.session_id());
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid host in URL").into());
                        }
                    } else {
                        error!("[Session:{}] Invalid URL format", self.session_id());
                        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid URL format").into());
                    }
                } else {
                    error!("[Session:{}] Unsupported URL scheme", self.session_id());
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Unsupported URL scheme").into());
                }
            } else {
                error!("[Session:{}] Missing host header and invalid URL", self.session_id());
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Missing host information").into());
            }
        };

        if self.domain_blocker.is_blocked(&host) {
            info!("[Session:{}] Blocked access to domain: {}", self.session_id(), host);
            
            if !is_connect {
                // HTTP 요청의 경우 직접 차단 페이지 반환
                // HTML 차단 페이지 생성
                let html = format!(
                    "<!DOCTYPE html>\
                    <html>\
                    <head>\
                        <title>사이트 접근 차단됨</title>\
                        <meta charset=\"UTF-8\">\
                        <style>\
                            body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}\
                            .container {{ max-width: 800px; margin: 40px auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}\
                            h1 {{ color: #e74c3c; margin-top: 0; }}\
                            .info {{ background-color: #f8f9fa; padding: 15px; border-left: 4px solid #e74c3c; margin: 20px 0; }}\
                            .button {{ display: inline-block; background-color: #3498db; color: white; padding: 10px 20px; text-decoration: none; border-radius: 4px; margin-top: 20px; }}\
                        </style>\
                    </head>\
                    <body>\
                        <div class=\"container\">\
                            <h1>접속이 차단되었습니다</h1>\
                            <p>관리자 정책에 따라 요청하신 사이트에 대한 접속이 차단되었습니다.</p>\
                            <div class=\"info\">\
                                <p><strong>차단된 도메인:</strong> {}</p>\
                                <p><strong>차단 시간:</strong> {}</p>\
                            </div>\
                            <p>문의사항이 있으시면 네트워크 관리자에게 연락하세요.</p>\
                            <!-- 이용 정책 페이지 URL (실제 정책 페이지로 변경 필요) -->\
                            <a href=\"https://example.com/acceptable-use-policy\" class=\"button\">이용 정책 확인하기</a>\
                        </div>\
                    </body>\
                    </html>", 
                    host, 
                    chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
                );
                
                // HTTP 응답 헤더와 본문 구성
                let blocked_message = format!(
                    "HTTP/1.1 200 OK\r\n\
                    Connection: close\r\n\
                    Content-Type: text/html; charset=UTF-8\r\n\
                    Content-Length: {}\r\n\
                    \r\n\
                    {}", 
                    html.len(), 
                    html
                );
                
                // 차단 메시지 전송 시도
                match client_stream.try_write(blocked_message.as_bytes()) {
                    Ok(_) => {
                        info!("[Session:{}] Successfully sent HTTP block page to client for {}", self.session_id(), host);
                        
                        // 데이터를 모두 전송하기 위해 flush 호출
                        if let Err(e) = client_stream.flush().await {
                            error!("[Session:{}] Failed to flush HTTP stream: {}", self.session_id(), e);
                        }
                        
                        // 잠시 대기 후 연결 종료 (클라이언트가 응답을 처리할 시간을 줌)
                        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                    },
                    Err(e) => error!("[Session:{}] Failed to send HTTP block page: {}", self.session_id(), e)
                }
                
                if let Some(pool) = self.buffer_pool {
                    pool.return_buffer(buffer);
                }
                
                return Ok(());
            } else {
                // HTTPS 요청(CONNECT)의 경우:
                // 1. 일단 CONNECT 요청을 승인하고 TLS 핸드셰이크 진행
                let response = "HTTP/1.1 200 Connection Established\r\nConnection: keep-alive\r\n\r\n";
                match client_stream.try_write(response.as_bytes()) {
                    Ok(_) => debug!("[Session:{}] Successfully sent CONNECT response", self.session_id()),
                    Err(e) => {
                        error!("[Session:{}] Failed to send CONNECT response: {}", self.session_id(), e);
                        return Err(e.into());
                    }
                }
                
                info!("[Session:{}] CONNECT request approved for {} (will be blocked after TLS handshake)", self.session_id(), host);
                
                // 2. 가짜 인증서로 클라이언트와 TLS 연결 수립
                let fake_cert = match generate_fake_cert(&host).await {
                    Ok(cert) => cert,
                    Err(e) => {
                        error!("[Session:{}] Failed to generate fake certificate: {}", self.session_id(), e);
                        return Err(e);
                    }
                };
                
                let mut tls_stream = match accept_tls_with_cert(client_stream, fake_cert).await {
                    Ok(stream) => stream,
                    Err(e) => {
                        error!("[Session:{}] Failed to establish TLS with client: {}", self.session_id(), e);
                        return Err(e);
                    }
                };
                
                info!("[Session:{}] Established TLS with client for blocked domain {}", self.session_id(), host);
                
                // 3. TLS 연결 후 HTML 차단 페이지 생성
                let html = format!(
                    "<!DOCTYPE html>\
                    <html>\
                    <head>\
                        <title>사이트 접근 차단됨 (HTTPS)</title>\
                        <meta charset=\"UTF-8\">\
                        <style>\
                            body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}\
                            .container {{ max-width: 800px; margin: 40px auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}\
                            h1 {{ color: #e74c3c; margin-top: 0; }}\
                            .info {{ background-color: #f8f9fa; padding: 15px; border-left: 4px solid #e74c3c; margin: 20px 0; }}\
                            .warning {{ background-color: #fff3cd; padding: 15px; border-left: 4px solid #ffc107; margin: 20px 0; }}\
                            .button {{ display: inline-block; background-color: #3498db; color: white; padding: 10px 20px; text-decoration: none; border-radius: 4px; margin-top: 20px; }}\
                        </style>\
                    </head>\
                    <body>\
                        <div class=\"container\">\
                            <h1>보안 연결이 차단되었습니다</h1>\
                            <p>관리자 정책에 따라 요청하신 보안 사이트(HTTPS)에 대한 접속이 차단되었습니다.</p>\
                            <div class=\"warning\">\
                                <p><strong>참고:</strong> 이 페이지는 TLS 연결이 성공적으로 수립된 후 표시됩니다. 프록시 서버에서 제공하는 인증서를 사용하여 암호화된 연결이 설정되었습니다.</p>\
                            </div>\
                            <div class=\"info\">\
                                <p><strong>차단된 도메인:</strong> {}</p>\
                                <p><strong>차단 시간:</strong> {}</p>\
                            </div>\
                            <p>문의사항이 있으시면 네트워크 관리자에게 연락하세요.</p>\
                            <!-- 이용 정책 페이지 URL (실제 정책 페이지로 변경 필요) -->\
                            <a href=\"https://example.com/acceptable-use-policy\" class=\"button\">이용 정책 확인하기</a>\
                        </div>\
                    </body>\
                    </html>", 
                    host, 
                    chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
                );
                
                // HTTP 응답 헤더와 본문 구성
                let blocked_html = format!(
                    "HTTP/1.1 403 Forbidden\r\n\
                    Connection: close\r\n\
                    Content-Type: text/html; charset=UTF-8\r\n\
                    Content-Length: {}\r\n\
                    \r\n\
                    {}", 
                    html.len(), 
                    html
                );
                
                // TLS 스트림으로 차단 페이지 전송
                match tls_stream.write_all(blocked_html.as_bytes()).await {
                    Ok(_) => {
                        info!("[Session:{}] Successfully sent TLS block page to client for {}", self.session_id(), host);
                        
                        // 데이터를 모두 전송하기 위해 flush 호출
                        if let Err(e) = tls_stream.flush().await {
                            error!("[Session:{}] Failed to flush TLS stream: {}", self.session_id(), e);
                        }
                        
                        // 잠시 대기 후 연결 종료 (클라이언트가 응답을 처리할 시간을 줌)
                        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                    },
                    Err(e) => error!("[Session:{}] Failed to send TLS block page: {}", self.session_id(), e)
                }
                
                // 연결 종료 시 활성 연결 카운트 감소
                self.metrics.connection_closed(true);
                info!("[Session:{}] Sent blocked page over TLS for {} and closing connection", self.session_id(), host);
                
                if let Some(pool) = self.buffer_pool {
                    pool.return_buffer(buffer);
                }
                
                return Ok(());
            }
        }

        self.metrics.increment_request_count(is_connect);

        if is_connect {
            // CONNECT 요청에 대한 승인 응답 전송
            let response = "HTTP/1.1 200 Connection Established\r\nConnection: keep-alive\r\n\r\n";
            client_stream.try_write(response.as_bytes())?;
            info!("[Session:{}] CONNECT request approved for {}", self.session_id(), host);
            
            // 1. 가짜 인증서로 클라이언트와 TLS 연결
            let fake_cert = generate_fake_cert(&host).await?;
            info!("[Session:{}] Generated fake certificate for {}", self.session_id(), host);
            
            let tls_stream = accept_tls_with_cert(client_stream, fake_cert).await?;
            info!("[Session:{}] Established TLS with client for {}", self.session_id(), host);
            
            // 2. 실제 서버와도 TLS 연결 - 설정 전달하여 인증서 검증 옵션 적용
            match connect_tls(&host, &self.config).await {
                Ok(real_tls_stream) => {
                    info!("[Session:{}] Connected to real server {} over TLS", self.session_id(), host);
                    
                    // 3. 중간에서 데이터 가로채기
                    proxy_tls_streams(tls_stream, real_tls_stream, Arc::clone(&self.metrics), &self.session_id(), &host).await?;
                    
                    // 연결 종료 시 활성 연결 카운트 감소
                    self.metrics.connection_closed(true);
                    info!("[Session:{}] Completed TLS proxy for {}", self.session_id(), host);
                },
                Err(e) => {
                    // UnknownIssuer 오류 발생 시 설정 안내
                    if e.to_string().contains("UnknownIssuer") {
                        error!("[Session:{}] 서버 인증서 검증 실패: {}", self.session_id(), e);
                    }
                    return Err(e);
                }
            }
        } else {
            // HTTP 요청 처리
            info!("[Session:{}] Processing HTTP request for {}", self.session_id(), host);
            
            // 서버에 연결
            let server_addr = format!("{}:{}", host, port);
            let server_stream = match TcpStream::connect(&server_addr).await {
                Ok(stream) => stream,
                Err(e) => {
                    error!("[Session:{}] Failed to connect to target server {}: {}", self.session_id(), server_addr, e);
                    return Err(e.into());
                }
            };
            
            // 서버에 요청 전달
            if let Err(e) = server_stream.try_write(&buffer[0..n]) {
                error!("[Session:{}] Failed to forward request to server: {}", self.session_id(), e);
                return Err(e.into());
            }
            
            // 버퍼 반환
            if let Some(ref pool) = self.buffer_pool {
                pool.return_buffer(buffer);
            }
            
            // 프록시 시작
            info!("[Session:{}] Starting HTTP proxy for {}", self.session_id(), host);
            proxy_http_streams(client_stream, server_stream, Arc::clone(&self.metrics), &self.session_id()).await?;
            
            // 연결 종료 시 활성 연결 카운트 감소
            self.metrics.connection_closed(false);
            info!("[Session:{}] Completed HTTP proxy for {}", self.session_id(), host);
        }

        debug!("[Session:{}] target host: {}, port: {}", self.session_id(), host, port);
        Ok(())
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