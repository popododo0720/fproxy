use std::error::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use log::{debug, error, info};
use chrono;

use crate::tls::{generate_fake_cert, accept_tls_with_cert};
use crate::REQUEST_LOGGER;

/// 차단 페이지 생성 및 전송을 담당하는 구조체
pub struct BlockPage;

impl BlockPage {
    /// 새로운 BlockPage 인스턴스 생성
    pub fn new() -> Self {
        Self {}
    }
    
    /// HTTP 차단 페이지 생성
    pub fn create_http_block_page(&self, host: &str) -> String {
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
                </div>\
            </body>\
            </html>", 
            host, 
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
        );
        
        format!(
            "HTTP/1.1 200 OK\r\n\
            Connection: close\r\n\
            Content-Type: text/html; charset=UTF-8\r\n\
            Content-Length: {}\r\n\
            \r\n\
            {}", 
            html.len(), 
            html
        )
    }
    
    /// HTTPS 차단 페이지 생성
    pub fn create_https_block_page(&self, host: &str) -> String {
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
                </div>\
            </body>\
            </html>", 
            host, 
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
        );
        
        format!(
            "HTTP/1.1 403 Forbidden\r\n\
            Connection: close\r\n\
            Content-Type: text/html; charset=UTF-8\r\n\
            Content-Length: {}\r\n\
            \r\n\
            {}", 
            html.len(), 
            html
        )
    }
    
    /// 차단 요청 로깅
    async fn log_blocked_request(&self, request_data: &str, host: &str, ip: &str, session_id: &str, is_tls: bool) {
        // 간단하게 try_read()로 돌아가되, 비동기적으로 처리
        if let Ok(logger) = REQUEST_LOGGER.try_read() {
            // 요청 파싱 및 로깅
            // 여기서는 RequestLogger의 parse_request_for_reject 메서드를 사용하는 것이 적절합니다
            logger.log_rejected_request(request_data, host, ip, session_id, is_tls).await;
            debug!("[Session:{}] 차단된 요청 로깅 시도 완료", session_id);
        } else {
            debug!("[Session:{}] RequestLogger 읽기 락 획득 실패", session_id);
        }
    }
    
    /// HTTP 차단 페이지 전송
    pub async fn send_http_block_page(&self, client_stream: &mut TcpStream, host: &str, session_id: &str, request: Option<&str>, client_ip: Option<&str>) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 요청 로깅
        if let (Some(req), Some(ip)) = (request, client_ip) {
            self.log_blocked_request(req, host, ip, session_id, false).await; // HTTP는 TLS 아님
        }
        
        let blocked_message = self.create_http_block_page(host);
        
        // 차단 메시지 전송 시도
        match client_stream.write_all(blocked_message.as_bytes()).await {
            Ok(_) => {
                info!("[Session:{}] Successfully sent HTTP block page to client for {}", session_id, host);
                
                // 데이터를 모두 전송하기 위해 flush 호출
                if let Err(e) = client_stream.flush().await {
                    error!("[Session:{}] Failed to flush HTTP stream: {}", session_id, e);
                }
                
                // 잠시 대기 후 연결 종료 (클라이언트가 응답을 처리할 시간을 줌)
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                Ok(())
            },
            Err(e) => {
                error!("[Session:{}] Failed to send HTTP block page: {}", session_id, e);
                Err(e.into())
            }
        }
    }
    
    /// HTTPS 차단 페이지 전송 (CONNECT 요청 처리 포함)
    pub async fn handle_https_block(&self, mut client_stream: TcpStream, host: &str, session_id: &str, request: Option<&str>, client_ip: Option<&str>) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 요청 로깅
        if let (Some(req), Some(ip)) = (request, client_ip) {
            self.log_blocked_request(req, host, ip, session_id, true).await; // HTTPS는 TLS임
        }
        
        // 1. 일단 CONNECT 요청을 승인하고 TLS 핸드셰이크 진행
        let response = "HTTP/1.1 200 Connection Established\r\nConnection: keep-alive\r\n\r\n";
        match client_stream.write_all(response.as_bytes()).await {
            Ok(_) => debug!("[Session:{}] Successfully sent CONNECT response", session_id),
            Err(e) => {
                error!("[Session:{}] Failed to send CONNECT response: {}", session_id, e);
                return Err(e.into());
            }
        }
        
        info!("[Session:{}] CONNECT request approved for {} (will be blocked after TLS handshake)", session_id, host);
        
        // 2. 가짜 인증서로 클라이언트와 TLS 연결 수립
        let fake_cert = match generate_fake_cert(host).await {
            Ok(cert) => cert,
            Err(e) => {
                error!("[Session:{}] Failed to generate fake certificate: {}", session_id, e);
                return Err(e);
            }
        };
        
        let mut tls_stream = match accept_tls_with_cert(client_stream, fake_cert).await {
            Ok(stream) => stream,
            Err(e) => {
                error!("[Session:{}] Failed to establish TLS with client: {}", session_id, e);
                return Err(e);
            }
        };
        
        info!("[Session:{}] Established TLS with client for blocked domain {}", session_id, host);
        
        // 3. TLS 연결 후 HTML 차단 페이지 전송
        let blocked_html = self.create_https_block_page(host);
        
        // TLS 스트림으로 차단 페이지 전송
        match tls_stream.write_all(blocked_html.as_bytes()).await {
            Ok(_) => {
                info!("[Session:{}] Successfully sent TLS block page to client for {}", session_id, host);
                
                // 데이터를 모두 전송하기 위해 flush 호출
                if let Err(e) = tls_stream.flush().await {
                    error!("[Session:{}] Failed to flush TLS stream: {}", session_id, e);
                }
                
                // 잠시 대기 후 연결 종료 (클라이언트가 응답을 처리할 시간을 줌)
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                Ok(())
            },
            Err(e) => {
                error!("[Session:{}] Failed to send TLS block page: {}", session_id, e);
                Err(e.into())
            }
        }
    }
} 