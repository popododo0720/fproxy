use log::{debug, error};
use tokio::io::AsyncWriteExt;
use tokio::fs::File;

use crate::constants::*;

/// TLS 요청 로깅을 담당하는 구조체
pub struct RequestLogger;

impl RequestLogger {
    /// 새로운 로그 파일 생성
    pub async fn create_log_file(host: &str, session_id: &str) -> Option<File> {
        let request_log_path = format!("logs/request_{}.log", host);
        match tokio::fs::File::create(&request_log_path).await {
            Ok(file) => Some(file),
            Err(e) => {
                error!("[Session:{}] Failed to create request log file: {}", session_id, e);
                None
            }
        }
    }

    /// TLS 요청 데이터 처리 및 로깅
    pub async fn process_request_data(
        data: &[u8],
        file: &mut File,
        accumulated_data: &mut Vec<u8>,
        processing_request: &mut bool,
        session_id: &str
    ) {
        // 데이터를 누적 버퍼에 추가
        accumulated_data.extend_from_slice(data);
        
        // 데이터를 문자열로 변환 시도
        if let Ok(data_str) = std::str::from_utf8(accumulated_data) {
            // HTTP 요청 시작 감지
            if !*processing_request && 
               (data_str.starts_with("GET ") || data_str.starts_with("POST ") || 
                data_str.starts_with("PUT ") || data_str.starts_with("DELETE ") || 
                data_str.starts_with("HEAD ") || data_str.starts_with("OPTIONS ")) {
                *processing_request = true;
            }
            
            // HTTP 요청 처리
            if *processing_request {
                // 헤더와 본문 분리 시도
                if data_str.contains("\r\n\r\n") {
                    let parts: Vec<&str> = data_str.split("\r\n\r\n").collect();
                    let headers = parts[0];
                    
                    // 헤더 라인 분리
                    let header_lines: Vec<&str> = headers.lines().collect();
                    
                    // 요청 라인 (첫 번째 줄)
                    let mut log_content = format!("{}\n", header_lines[0]);
                    
                    // Content-Length 값 추출
                    let mut content_length = 0;
                    let is_post = header_lines[0].starts_with("POST ");
                    
                    // 필요한 헤더 추출
                    for line in &header_lines[1..] {
                        if line.to_lowercase().starts_with("host:") ||
                           line.to_lowercase().starts_with("user-agent:") ||
                           line.to_lowercase().starts_with("referer:") ||
                           line.to_lowercase().starts_with("sec-ch-ua-platform:") {
                            log_content.push_str(&format!("{}\n", line));
                        }
                        
                        // Content-Length 값 추출
                        if is_post && line.to_lowercase().starts_with("content-length:") {
                            log_content.push_str(&format!("{}\n", line));
                            if let Some(len_str) = line.split(':').nth(1) {
                                if let Ok(len) = len_str.trim().parse::<usize>() {
                                    content_length = len;
                                }
                            }
                        }
                        
                        // Content-Type 헤더 추가 (POST 요청인 경우)
                        if is_post && line.to_lowercase().starts_with("content-type:") {
                            log_content.push_str(&format!("{}\n", line));
                        }
                    }
                    
                    // POST 요청이고 본문이 있는 경우
                    if is_post && parts.len() > 1 {
                        let body = parts[1];
                        
                        // 본문 전체를 받았는지 확인
                        let received_body_len = body.len();
                        
                        // 전체 본문을 받았거나 충분한 양을 받았으면 로깅
                        if content_length == 0 || received_body_len >= content_length || received_body_len >= 4096 {
                            // 본문 추가 (최대 4096)
                            let body_preview = if body.len() > 4096 {
                                format!("{}... (truncated)", &body[0..4096])
                            } else {
                                body.to_string()
                            };
                            
                            log_content.push_str("\n"); // 헤더와 본문 사이 빈 줄
                            log_content.push_str(&body_preview);
                            
                            // 로그 파일에 기록
                            if let Err(e) = file.write_all(format!("{}\n\n", log_content).as_bytes()).await {
                                error!("[Session:{}] Failed to write to request log file: {}", session_id, e);
                            }
                            
                            // 처리 완료 후 버퍼 초기화
                            accumulated_data.clear();
                            *processing_request = false;
                        }
                    } else {
                        // GET 요청이나 본문이 없는 요청은 바로 로깅
                        if let Err(e) = file.write_all(format!("{}\n\n", log_content).as_bytes()).await {
                            error!("[Session:{}] Failed to write to request log file: {}", session_id, e);
                        }
                        
                        // 처리 완료 후 버퍼 초기화
                        accumulated_data.clear();
                        *processing_request = false;
                    }
                }
            }
        }
        
        // 버퍼가 너무 커지면 초기화 (메모리 누수 방지)
        if accumulated_data.len() > BUFFER_SIZE_LARGE {
            accumulated_data.clear();
            *processing_request = false;
            debug!("[Session:{}] Accumulated buffer too large, clearing", session_id);
        }
    }
} 