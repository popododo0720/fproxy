use std::error::Error;
use std::sync::Arc;
use std::io;

use log::{debug, error, info};
use tokio::net::TcpStream;
use tokio_rustls::{server::TlsStream as ServerTlsStream, client::TlsStream as ClientTlsStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::metrics::Metrics;
use crate::constants::*;
use crate::acl::request_logger::RequestLogger;

/// TLS 스트림 간에 데이터를 전달하고 검사합니다
pub async fn proxy_tls_streams(
    client_stream: ServerTlsStream<TcpStream>,
    server_stream: ClientTlsStream<TcpStream>,
    metrics: Arc<Metrics>,
    session_id: &str,
    host: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // 양방향 데이터 전송 및 검사 로직 구현
    let (mut client_read, mut client_write) = tokio::io::split(client_stream);
    let (mut server_read, mut server_write) = tokio::io::split(server_stream);
    
    // 바이트 카운터 초기화
    let client_to_server = async {
        let mut buffer = vec![0u8; BUFFER_SIZE_MEDIUM];
        let mut total_bytes = 0u64;
        
        // 요청 저장을 위한 파일 생성
        let mut request_log_file = RequestLogger::create_log_file(host, session_id).await;
        
        // 누적 버퍼 (HTTP 요청이 여러 패킷으로 나뉘어 올 경우를 대비)
        let mut accumulated_data = Vec::new();
        let mut processing_request = false;
        
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
                    
                    // 복호화된 데이터 처리 및 중간 수준 로깅
                    if let Some(file) = &mut request_log_file {
                        RequestLogger::process_request_data(&buffer[0..n], file, &mut accumulated_data, &mut processing_request, session_id).await;
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
        Ok::<u64, io::Error>(total_bytes)
    };
    
    let server_to_client = async {
        let mut buffer = vec![0u8; BUFFER_SIZE_SMALL];
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