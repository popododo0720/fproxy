use std::error::Error;
use std::sync::Arc;
use chrono::Utc;
use log::{debug, error, info};

use crate::logging::message::{LogMessage, LogPriority};
use crate::logging::worker::WorkerPool;

/// 로거 인터페이스
#[derive(Clone)]
pub struct Logger {
    /// 워커 풀 참조
    worker_pool: Option<Arc<WorkerPool>>,
    /// 초기화 완료 여부
    initialized: bool,
}

impl Logger {
    /// 새 로거 인스턴스 생성
    pub fn new() -> Self {
        Self {
            worker_pool: None,
            initialized: false,
        }
    }
    
    /// 로거 초기화 상태 확인
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }
    
    /// 로거 초기화
    pub async fn init(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 이미 초기화되었는지 확인
        if self.initialized {
            debug!("Logger 이미 초기화됨");
            return Ok(());
        }
        
        debug!("Logger 초기화 시작...");
        
        // 워커 풀 생성
        let worker_pool = WorkerPool::new().await?;
        let worker_pool = Arc::new(worker_pool);
        
        // 로거 설정
        self.worker_pool = Some(worker_pool.clone());
        self.initialized = true;
        
        info!("Logger 초기화 완료");
        Ok(())
    }
    
    /// 비동기 요청 로깅
    pub async fn log_request<'a>(
        &self,
        host: impl Into<String>,
        method: impl Into<String>,
        path: impl Into<String>,
        header: impl Into<String>,
        body: Option<String>,
        session_id: impl Into<String>,
        client_ip: impl Into<String>,
        target_ip: impl Into<String>,
        is_rejected: bool,
        is_tls: bool
    ) -> Result<(), &'static str> {
        // 초기화 여부 확인
        if !self.initialized {
            debug!("초기화되지 않은 Logger에 로깅 시도");
            return Err("로거가 초기화되지 않았습니다");
        }
        
        if let Some(worker_pool) = &self.worker_pool {
            let method_str = method.into();
            
            // 우선순위 결정
            let priority = LogPriority::from_request_info(is_rejected, &method_str);
            
            let log_message = LogMessage::RequestLog {
                host: host.into(),
                method: method_str,
                path: path.into(),
                header: header.into(),
                body,
                timestamp: Utc::now(),
                session_id: session_id.into(),
                client_ip: client_ip.into(),
                target_ip: target_ip.into(),
                is_rejected,
                is_tls,
                priority,
            };
            
            // 비동기로 메시지 전송
            match worker_pool.send_log(log_message).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    error!("로그 메시지 전송 실패: {}", e);
                    Err("로그 메시지 전송 실패")
                }
            }
        } else {
            error!("초기화는 되었지만 worker_pool이 없습니다");
            Err("로거 내부 오류: worker_pool이 없습니다")
        }
    }
    
    /// 비동기 응답 로깅
    pub async fn log_response(
        &self,
        session_id: impl Into<String>,
        status_code: u16,
        response_time: u64,
        response_size: usize,
        headers: impl Into<String>,
        body_preview: Option<String>,
    ) -> Result<(), &'static str> {
        // 초기화 여부 확인
        if !self.initialized {
            debug!("초기화되지 않은 Logger에 응답 로깅 시도");
            return Err("로거가 초기화되지 않았습니다");
        }
        
        if let Some(worker_pool) = &self.worker_pool {
            // 우선순위 결정
            let base_priority = LogPriority::from_status_code(status_code);
            let priority = LogPriority::from_response_time(response_time, base_priority);
            
            let log_message = LogMessage::ResponseLog {
                session_id: session_id.into(),
                status_code,
                response_time,
                response_size,
                timestamp: Utc::now(),
                headers: headers.into(),
                body_preview,
                priority,
            };
            
            // 비동기로 메시지 전송
            match worker_pool.send_log(log_message).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    error!("응답 로그 메시지 전송 실패: {}", e);
                    Err("응답 로그 메시지 전송 실패")
                }
            }
        } else {
            error!("초기화는 되었지만 worker_pool이 없습니다");
            Err("로거 내부 오류: worker_pool이 없습니다")
        }
    }
    
    /// 차단된 요청 로깅
    pub async fn log_rejected_request(&self, request: &str, host: &str, ip: &str, session_id: &str, is_tls: bool) -> Result<(), &'static str> {
        // 초기화 여부 확인
        if !self.initialized {
            debug!("초기화되지 않은 Logger에 차단된 요청 로깅 시도");
            return Err("로거가 초기화되지 않았습니다");
        }
        
        // 요청 파싱
        let (method, path, header, body) = self.parse_request_for_reject(request);
        
        // log_request 메서드 호출하여 로깅
        self.log_request(
            host,
            method,
            path,
            header,
            body,
            session_id,
            ip,
            "Blocked", // 차단된 요청은 타겟 IP를 "Blocked"로 표시
            true,      // 차단된 요청
            is_tls     // TLS 여부
        ).await
    }
    
    /// 요청 문자열 파싱
    fn parse_request_for_reject(&self, request: &str) -> (String, String, String, Option<String>) {
        let mut method = "UNKNOWN".to_string();
        let mut path = "/".to_string();
        let mut header = request.to_string();
        let mut body = None;
        
        // 요청 라인 추출 및 메서드, 경로 파싱
        if let Some(first_line) = request.lines().next() {
            let parts: Vec<&str> = first_line.split_whitespace().collect();
            if parts.len() >= 2 {
                method = parts[0].to_string();
                path = parts[1].to_string();
            }
        }
        
        // 헤더와 본문 분리
        if let Some(header_end) = request.find("\r\n\r\n") {
            header = request[..header_end].to_string();
            
            // 본문 추출 (있는 경우)
            let body_start = header_end + 4; // "\r\n\r\n" 길이
            if body_start < request.len() {
                body = Some(request[body_start..].to_string());
            }
        }
        
        (method, path, header, body)
    }
    
    /// 로그 플러시 요청
    pub async fn flush(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 초기화 여부 확인
        if !self.initialized {
            debug!("초기화되지 않은 Logger에 플러시 시도");
            return Err("로거가 초기화되지 않았습니다".into());
        }
        
        if let Some(worker_pool) = &self.worker_pool {
            worker_pool.flush().await
        } else {
            error!("초기화는 되었지만 worker_pool이 없습니다");
            Err("로거 내부 오류: worker_pool이 없습니다".into())
        }
    }
} 