use chrono::{DateTime, Utc};

/// 로그 메시지 타입
#[derive(Debug, Clone)]
pub enum LogMessage {
    /// 요청 로그 메시지
    RequestLog { 
        host: String,
        method: String,
        path: String, 
        header: String,
        body: Option<String>,
        timestamp: DateTime<Utc>, 
        session_id: String,
        client_ip: String,
        target_ip: String,
        is_rejected: bool,
        is_tls: bool,
        priority: LogPriority,
    },
    
    /// 응답 로그 메시지
    ResponseLog {
        session_id: String,
        status_code: u16,
        response_time: u64,
        response_size: usize,
        timestamp: DateTime<Utc>,
        headers: String,
        body_preview: Option<String>,
        priority: LogPriority,
    },
    
    /// 로그 플러시 명령
    FlushLogs,
}

/// 로그 우선순위 정의
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogPriority {
    Low,    // 일반 로그 (정상적인 요청/응답)
    Medium, // 중요 로그 (에러 상태코드, 지연 응답 등)
    High,   // 긴급 로그 (보안 관련, 차단된 요청 등)
}

impl LogPriority {
    /// 응답 상태 코드에 따른 우선순위 결정
    pub fn from_status_code(status_code: u16) -> Self {
        match status_code {
            200..=399 => LogPriority::Low,     // 성공 응답은 낮은 우선순위
            400..=499 => LogPriority::Medium,  // 클라이언트 에러는 중간 우선순위
            500..=599 => LogPriority::High,    // 서버 에러는 높은 우선순위
            _ => LogPriority::Medium,          // 기타 상태 코드는 중간 우선순위
        }
    }
    
    /// 요청 특성에 따른 우선순위 결정
    pub fn from_request_info(is_rejected: bool, method: &str) -> Self {
        if is_rejected {
            return LogPriority::High;  // 차단된 요청은 높은 우선순위
        }
        
        // POST, PUT, DELETE 등의 요청은 중간 우선순위
        match method {
            "GET" | "HEAD" | "OPTIONS" => LogPriority::Low,
            _ => LogPriority::Medium,
        }
    }
    
    /// 응답 시간에 따른 우선순위 상향 조정
    pub fn from_response_time(response_time: u64, base_priority: LogPriority) -> Self {
        // 응답 시간이 길면 우선순위 상향
        if response_time > 1000 {  // 1초 이상
            match base_priority {
                LogPriority::Low => LogPriority::Medium,
                _ => LogPriority::High,
            }
        } else {
            base_priority
        }
    }
} 