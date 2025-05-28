use log::{debug, error, info};
use tokio::io::AsyncWriteExt;
use tokio::fs::File;
use tokio::sync::mpsc::{self, Sender, Receiver};
use std::collections::{VecDeque, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use chrono::{Local, Datelike};

use crate::constants::*;
use crate::metrics::Metrics;

// 최대 최근 응답 시간 저장 개수
const MAX_RECENT_RESPONSES: usize = 1000;

/// 로그 메시지 타입
#[derive(Debug)]
pub enum LogMessage {
    RequestLog { content: String, host: String },
    RejectLog { content: String, host: String, ip: String },
}

/// 응답 시간 정보 구조체
#[derive(Debug, Clone, Copy)]
struct ResponseTimeInfo {
    timestamp: SystemTime,
    duration_ms: u64,
}

/// 응답 시간 메트릭을 저장하는 구조체
#[derive(Debug, Default)]
pub struct ResponseMetrics {
    // 최근 1분간 응답 시간 기록 (timestamp, duration_ms)
    recent_responses: VecDeque<ResponseTimeInfo>,
    total_responses: usize,
    total_duration_ms: u64,
    min_duration_ms: u64,
    max_duration_ms: u64,
}

impl ResponseMetrics {
    /// 새 ResponseMetrics 인스턴스 생성
    pub fn new() -> Self {
        ResponseMetrics {
            recent_responses: VecDeque::with_capacity(MAX_RECENT_RESPONSES),
            total_responses: 0,
            total_duration_ms: 0,
            min_duration_ms: u64::MAX,
            max_duration_ms: 0,
        }
    }

    /// 응답 시간 추가
    pub fn add_response_time(&mut self, duration_ms: u64) {
        self.clean_old_responses();
        
        // 통계 업데이트
        self.total_responses += 1;
        self.total_duration_ms += duration_ms;
        
        // 최소/최대 업데이트
        if duration_ms < self.min_duration_ms {
            self.min_duration_ms = duration_ms;
        }
        if duration_ms > self.max_duration_ms {
            self.max_duration_ms = duration_ms;
        }
        
        // 최근 응답 시간 추가
        self.recent_responses.push_back(ResponseTimeInfo {
            timestamp: SystemTime::now(),
            duration_ms,
        });
    }
    
    /// 1분 이상 지난 응답 제거
    fn clean_old_responses(&mut self) {
        if let Ok(now) = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            let one_minute_ago = now - Duration::from_secs(60);
            
            // 1분 이상 지난 응답 제거
            while let Some(front) = self.recent_responses.front() {
                if let Ok(timestamp) = front.timestamp.duration_since(SystemTime::UNIX_EPOCH) {
                    if timestamp < one_minute_ago {
                        self.recent_responses.pop_front();
                    } else {
                        break;
                    }
                } else {
                    // 타임스탬프 오류 발생 시 제거
                    self.recent_responses.pop_front();
                }
            }
        }
    }
    
    // /// 전체 평균 응답 시간 계산
    // pub fn get_average_response_time(&self) -> Option<f64> {
    //     if self.total_responses > 0 {
    //         Some(self.total_duration_ms as f64 / self.total_responses as f64)
    //     } else {
    //         None
    //     }
    // }
    
    // /// 최근 1분 평균 응답 시간 계산
    // pub fn get_recent_average_response_time(&self) -> Option<f64> {
    //     if !self.recent_responses.is_empty() {
    //         let total: u64 = self.recent_responses.iter()
    //             .map(|info| info.duration_ms)
    //             .sum();
    //         Some(total as f64 / self.recent_responses.len() as f64)
    //     } else {
    //         None
    //     }
    // }
    
    // /// 현재 통계 반환
    // pub fn get_stats(&self) -> (usize, Option<f64>, Option<f64>, u64, u64) {
    //     (
    //         self.total_responses,
    //         self.get_average_response_time(),
    //         self.get_recent_average_response_time(),
    //         self.min_duration_ms,
    //         self.max_duration_ms
    //     )
    // }
    
    // /// 응답 시간 통계 출력
    // pub fn print_stats(&self) {
    //     info!("=== Response Time Statistics ===");
    //     info!("Total responses: {}", self.total_responses);
        
    //     if self.total_responses > 0 {
    //         info!("Overall average response time: {:.2} ms", 
    //              self.get_average_response_time().unwrap_or(0.0));
    //         info!("Min response time: {} ms", self.min_duration_ms);
    //         info!("Max response time: {} ms", self.max_duration_ms);
    //     }
        
    //     if let Some(recent_avg) = self.get_recent_average_response_time() {
    //         info!("Recent (1 min) average response time: {:.2} ms", recent_avg);
    //         info!("Recent responses count: {}", self.recent_responses.len());
    //     }
    // }
}

/// TLS 요청 로깅을 담당하는 구조체
pub struct RequestLogger {
    metrics: Arc<Mutex<ResponseMetrics>>,
    request_start_times: Arc<Mutex<HashMap<String, Instant>>>,
    log_sender: Option<Sender<LogMessage>>,
    global_metrics: Arc<Metrics>,
}

impl RequestLogger {
    /// 새 RequestLogger 인스턴스 생성
    pub fn new() -> Self {
        RequestLogger {
            metrics: Arc::new(Mutex::new(ResponseMetrics::new())),
            request_start_times: Arc::new(Mutex::new(HashMap::new())),
            log_sender: None,
            global_metrics: Metrics::new(),
        }
    }
    
    /// 로그 작성기 초기화
    pub async fn init(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 로그 디렉터리 생성
        tokio::fs::create_dir_all("logs").await?;
        
        // 채널 생성
        let (tx, rx) = mpsc::channel::<LogMessage>(1000);
        self.log_sender = Some(tx);
        
        // 로그 작성 태스크 시작
        tokio::spawn(Self::log_writer_task(rx));
        
        info!("RequestLogger initialized with async logging");
        Ok(())
    }
    
    /// 로그 작성 태스크
    async fn log_writer_task(mut rx: Receiver<LogMessage>) {
        let mut file_cache: HashMap<String, File> = HashMap::new();
        
        while let Some(message) = rx.recv().await {
            match message {
                LogMessage::RequestLog { content, host } => {
                    // 호스트에 해당하는 파일이 없으면 생성
                    if !file_cache.contains_key(&host) {
                        let today = Local::now();
                        let date_str = format!("{:04}{:02}{:02}", 
                            today.year(), today.month(), today.day());
                        let log_path = format!("logs/request_{}_{}.log", host, date_str);
                        
                        match tokio::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(&log_path)
                            .await {
                                Ok(file) => {
                                    file_cache.insert(host.clone(), file);
                                },
                                Err(e) => {
                                    error!("Failed to open log file for {}: {}", host, e);
                                    continue;
                                }
                            }
                    }
                    
                    // 파일에 로그 작성
                    if let Some(file) = file_cache.get_mut(&host) {
                        if let Err(e) = file.write_all(content.as_bytes()).await {
                            error!("Failed to write to log file: {}", e);
                        }
                    }
                },
                LogMessage::RejectLog { content, host, ip } => {
                    // 차단 로그 파일 이름 생성
                    let today = Local::now();
                    let date_str = format!("{:04}{:02}{:02}", 
                        today.year(), today.month(), today.day());
                    let log_key = format!("{}_{}_reject", host, date_str);
                    
                    if !file_cache.contains_key(&log_key) {
                        let log_path = format!("logs/request_{}_reject.log", host);
                        
                        match tokio::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(&log_path)
                            .await {
                                Ok(file) => {
                                    file_cache.insert(log_key.clone(), file);
                                },
                                Err(e) => {
                                    error!("Failed to open reject log file for {}: {}", host, e);
                                    continue;
                                }
                            }
                    }
                    
                    // IP 정보와 함께 로그 작성
                    let log_content = format!("# IP: {}\n{}\n\n", ip, content);
                    
                    // 파일에 로그 작성
                    if let Some(file) = file_cache.get_mut(&log_key) {
                        if let Err(e) = file.write_all(log_content.as_bytes()).await {
                            error!("Failed to write to reject log file: {}", e);
                        }
                    }
                }
            }
        }
    }
    
    /// 새로운 로그 파일 생성
    pub async fn create_log_file(host: &str, session_id: &str) -> Option<File> {
        // 로그 디렉터리 생성 확인
        if let Err(e) = tokio::fs::create_dir_all("logs").await {
            error!("[Session:{}] Failed to create logs directory: {}", session_id, e);
            return None;
        }
        
        let request_log_path = format!("logs/request_{}.log", host);
        match tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&request_log_path)
            .await {
                Ok(file) => Some(file),
                Err(e) => {
                    error!("[Session:{}] Failed to create request log file: {}", session_id, e);
                    None
                }
            }
    }

    /// 요청 시작 시간 기록
    pub fn record_request_start(&self, request_id: &str) {
        if let Ok(mut start_times) = self.request_start_times.lock() {
            start_times.insert(request_id.to_string(), Instant::now());
        }
    }
    
    /// 응답 완료 시간 기록 및 메트릭 업데이트
    pub fn record_request_end(&self, request_id: &str) -> Option<u64> {
        if let Ok(mut start_times) = self.request_start_times.lock() {
            if let Some(start_time) = start_times.remove(request_id) {
                let duration = start_time.elapsed();
                let duration_ms = duration.as_millis() as u64;
                
                // 로컬 메트릭 업데이트
                if let Ok(mut metrics) = self.metrics.lock() {
                    metrics.add_response_time(duration_ms);
                    
                    // 전역 메트릭스 업데이트
                    self.global_metrics.update_response_time_stats(duration_ms);
                    
                }
                
                return Some(duration_ms);
            }
        }
        None
    }
    
    /// 비동기로 로그 작성
    pub fn log_async(&self, content: String, host: String) -> Result<(), &'static str> {
        if let Some(sender) = &self.log_sender {
            let message = LogMessage::RequestLog { content, host };
            if sender.try_send(message).is_err() {
                return Err("로그 채널이 가득 찼습니다");
            }
            Ok(())
        } else {
            Err("로그 작성기가 초기화되지 않았습니다")
        }
    }
    
    /// 비동기로 차단 로그 작성
    pub fn log_reject_async(&self, content: String, host: String, ip: String) -> Result<(), &'static str> {
        if let Some(sender) = &self.log_sender {
            let message = LogMessage::RejectLog { content, host, ip };
            if sender.try_send(message).is_err() {
                return Err("로그 채널이 가득 찼습니다");
            }
            Ok(())
        } else {
            Err("로그 작성기가 초기화되지 않았습니다")
        }
    }

    /// TLS 요청 데이터 처리 및 로깅
    pub async fn process_request_data(
        &self,
        data: &[u8],
        file: &mut Option<File>,
        accumulated_data: &mut Vec<u8>,
        processing_request: &mut bool,
        session_id: &str,
        request_id: &str,
        host: &str,
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
                // 요청 시작 시간 기록
                self.record_request_start(request_id);
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
                            
                            // 응답 시간 기록
                            if let Some(duration_ms) = self.record_request_end(request_id) {
                                log_content.push_str(&format!("\nResponse-Time: {} ms\n", duration_ms));
                            }
                            
                            // 로그 작성 (파일이 있으면 직접, 없으면 비동기)
                            if let Some(file) = file {
                                if let Err(e) = file.write_all(format!("{}\n\n", log_content).as_bytes()).await {
                                    error!("[Session:{}] Failed to write to request log file: {}", session_id, e);
                                }
                            } else {
                                // 비동기 로깅 시도
                                if let Err(e) = self.log_async(format!("{}\n\n", log_content), host.to_string()) {
                                    error!("[Session:{}] Failed to log asynchronously: {}", session_id, e);
                                }
                            }
                            
                            // 처리 완료 후 버퍼 초기화
                            accumulated_data.clear();
                            *processing_request = false;
                        }
                    } else {
                        // GET 요청이나 본문이 없는 요청은 바로 로깅
                        // 응답 시간 기록
                        if let Some(duration_ms) = self.record_request_end(request_id) {
                            log_content.push_str(&format!("\nResponse-Time: {} ms\n", duration_ms));
                        }
                        
                        // 로그 작성 (파일이 있으면 직접, 없으면 비동기)
                        if let Some(file) = file {
                            if let Err(e) = file.write_all(format!("{}\n\n", log_content).as_bytes()).await {
                                error!("[Session:{}] Failed to write to request log file: {}", session_id, e);
                            }
                        } else {
                            // 비동기 로깅 시도
                            if let Err(e) = self.log_async(format!("{}\n\n", log_content), host.to_string()) {
                                error!("[Session:{}] Failed to log asynchronously: {}", session_id, e);
                            }
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
    
    /// 차단된 요청 로깅
    pub async fn log_rejected_request(&self, request: &str, host: &str, ip: &str, session_id: &str) {
        let mut log_content = String::new();
        
        // 헤더 라인 분리
        let lines: Vec<&str> = request.lines().collect();
        if !lines.is_empty() {
            // 요청 라인 (첫 번째 줄)
            log_content.push_str(&format!("{} [BLOCKED]\n", lines[0]));
            
            // 필요한 헤더 추출
            for line in &lines[1..] {
                if line.to_lowercase().starts_with("host:") ||
                   line.to_lowercase().starts_with("user-agent:") ||
                   line.to_lowercase().starts_with("referer:") ||
                   line.to_lowercase().starts_with("sec-ch-ua-platform:") {
                    log_content.push_str(&format!("{}\n", line));
                }
            }
            
            // 비동기 로깅 시도
            if let Err(e) = self.log_reject_async(
                log_content, 
                host.to_string(),
                ip.to_string()
            ) {
                error!("[Session:{}] Failed to log rejected request: {}", session_id, e);
            }
        }
    }
} 