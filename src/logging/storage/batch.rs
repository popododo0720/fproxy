use std::sync::Arc;
use tokio::sync::Mutex;
use chrono::{DateTime, Utc};

/// 요청 로그 배치
#[derive(Default)]
pub struct RequestLogBatch {
    pub logs: Vec<(String, String, String, String, Option<String>, DateTime<Utc>, String, String, String, bool, bool)>,
    pub size: usize,
}

impl RequestLogBatch {
    /// 새 배치 생성
    pub fn new() -> Self {
        Self {
            logs: Vec::with_capacity(100),
            size: 0,
        }
    }
    
    /// 배치에 로그 추가
    pub fn add_log(
        &mut self,
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
    ) -> usize {
        // 로그 항목 크기 계산
        let item_size = header.len() + body.as_ref().map_or(0, |b| b.len());
        
        // 로그 추가
        self.logs.push((
            host, method, path, header, body, timestamp,
            session_id, client_ip, target_ip, is_rejected, is_tls
        ));
        
        // 배치 크기 업데이트
        self.size += item_size;
        item_size
    }
    
    /// 배치 비우기
    pub fn clear(&mut self) {
        self.logs.clear();
        self.size = 0;
    }
    
    /// 배치가 비어있는지 확인
    pub fn is_empty(&self) -> bool {
        self.logs.is_empty()
    }
    
    /// 배치 크기 반환
    pub fn count(&self) -> usize {
        self.logs.len()
    }
}

/// 응답 로그 배치
#[derive(Default)]
pub struct ResponseLogBatch {
    pub logs: Vec<(String, u16, u64, usize, DateTime<Utc>, String, Option<String>)>,
    pub size: usize,
}

impl ResponseLogBatch {
    /// 새 배치 생성
    pub fn new() -> Self {
        Self {
            logs: Vec::with_capacity(100),
            size: 0,
        }
    }
    
    /// 배치에 로그 추가
    pub fn add_log(
        &mut self,
        session_id: String,
        status_code: u16,
        response_time: u64,
        response_size: usize,
        timestamp: DateTime<Utc>,
        headers: String,
        body_preview: Option<String>,
    ) -> usize {
        // 로그 항목 크기 계산
        let item_size = headers.len() + body_preview.as_ref().map_or(0, |b| b.len());
        
        // 로그 추가
        self.logs.push((
            session_id, status_code, response_time, response_size,
            timestamp, headers, body_preview
        ));
        
        // 배치 크기 업데이트
        self.size += item_size;
        item_size
    }
    
    /// 배치 비우기
    pub fn clear(&mut self) {
        self.logs.clear();
        self.size = 0;
    }
    
    /// 배치가 비어있는지 확인
    pub fn is_empty(&self) -> bool {
        self.logs.is_empty()
    }
    
    /// 배치 크기 반환
    pub fn count(&self) -> usize {
        self.logs.len()
    }
}

/// 스레드 안전한 요청 로그 배치
pub type ThreadSafeRequestBatch = Arc<Mutex<RequestLogBatch>>;

/// 스레드 안전한 응답 로그 배치
pub type ThreadSafeResponseBatch = Arc<Mutex<ResponseLogBatch>>;

/// 새로운 스레드 안전한 요청 로그 배치 생성
pub fn new_request_batch() -> ThreadSafeRequestBatch {
    Arc::new(Mutex::new(RequestLogBatch::new()))
}

/// 새로운 스레드 안전한 응답 로그 배치 생성
pub fn new_response_batch() -> ThreadSafeResponseBatch {
    Arc::new(Mutex::new(ResponseLogBatch::new()))
} 