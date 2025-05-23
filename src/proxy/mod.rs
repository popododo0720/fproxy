use std::error::Error;
use std::sync::Arc;
use std::io;

use log::{debug, error, info};
use tokio::net::TcpStream;
use tokio_rustls::{server::TlsStream as ServerTlsStream, client::TlsStream as ClientTlsStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::metrics::Metrics;

// 버퍼 크기
const BUFFER_SIZE: usize = 64 * 1024; // 64KB

/// HTTP 스트림 간에 데이터를 전달하고 검사합니다
pub async fn proxy_http_streams(
    client_stream: TcpStream,
    server_stream: TcpStream,
    metrics: Arc<Metrics>,
    session_id: &str
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (mut client_read, mut client_write) = tokio::io::split(client_stream);
    let (mut server_read, mut server_write) = tokio::io::split(server_stream);
    
    // 양방향 데이터 전송 설정
    let client_to_server = async {
        let mut buffer = vec![0u8; BUFFER_SIZE];
        let mut total_bytes = 0u64;
        
        loop {
            match client_read.read(&mut buffer).await {
                Ok(0) => break, // 연결 종료
                Ok(n) => {
                    // 서버로 데이터 전송
                    if let Err(e) = server_write.write_all(&buffer[0..n]).await {
                        error!("[Session:{}] Failed to write to server: {}", session_id, e);
                        break;
                    }
                    
                    // 메트릭스 업데이트 (실시간)
                    total_bytes += n as u64;
                    metrics.add_http_bytes_in(n as u64);
                    
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
    };
    
    let server_to_client = async {
        let mut buffer = vec![0u8; BUFFER_SIZE];
        let mut total_bytes = 0u64;
        
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
                    metrics.add_http_bytes_out(n as u64);
                    
                    // 주기적으로 로그 출력 (1MB 마다)
                    if total_bytes % (1024 * 1024) < (n as u64) {
                        debug!("[Session:{}] HTTP 서버→클라이언트 누적: {} KB", 
                               session_id, total_bytes / 1024);
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
    
    info!("[Session:{}] HTTP proxy completed", session_id);
    Ok(())
}

/// TLS 스트림 간에 데이터를 전달하고 검사합니다
pub async fn proxy_tls_streams(
    client_stream: ServerTlsStream<TcpStream>,
    server_stream: ClientTlsStream<TcpStream>,
    metrics: Arc<Metrics>,
    session_id: &str
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // 양방향 데이터 전송 및 검사 로직 구현
    let (mut client_read, mut client_write) = tokio::io::split(client_stream);
    let (mut server_read, mut server_write) = tokio::io::split(server_stream);
    
    // 바이트 카운터 초기화
    let client_to_server = async {
        let mut buffer = vec![0u8; BUFFER_SIZE];
        let mut total_bytes = 0u64;
        
        loop {
            match client_read.read(&mut buffer).await {
                Ok(0) => break, // 연결 종료
                Ok(n) => {
                    // 서버로 데이터 전송
                    if let Err(e) = server_write.write_all(&buffer[0..n]).await {
                        error!("[Session:{}] Failed to write to server: {}", session_id, e);
                        break;
                    }
                    
                    // 메트릭스 업데이트 (실시간)
                    total_bytes += n as u64;
                    metrics.add_tls_bytes_in(n as u64);
                    
                    // 주기적으로 로그 출력 (1MB 마다)
                    if total_bytes % (1024 * 1024) < (n as u64) {
                        debug!("[Session:{}] TLS 클라이언트→서버 누적: {} KB", 
                               session_id, total_bytes / 1024);
                    }
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
        info!("[Session:{}] 최종 클라이언트→서버 전송량: {} bytes", session_id, total_bytes);
        Ok::<u64, io::Error>(total_bytes)
    };
    
    let server_to_client = async {
        let mut buffer = vec![0u8; BUFFER_SIZE];
        let mut total_bytes = 0u64;
        
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
                    metrics.add_tls_bytes_out(n as u64);
                    
                    // 주기적으로 로그 출력 (1MB 마다)
                    if total_bytes % (1024 * 1024) < (n as u64) {
                        debug!("[Session:{}] TLS 서버→클라이언트 누적: {} KB", 
                               session_id, total_bytes / 1024);
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
    
    // 연결이 종료되었지만, TLS close_notify 오류는 정상 종료로 처리
    if let Err(e) = &result {
        if e.to_string().contains("peer closed connection without sending TLS close_notify") {
            return Ok(());
        }
    }
    
    info!("[Session:{}] TLS proxy completed", session_id);
    Ok(result?)
} 