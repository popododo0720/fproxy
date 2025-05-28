use log::{debug, error, info};
use tokio::io::AsyncWriteExt;
use tokio::fs::File;
use tokio::sync::mpsc::{self, Sender, Receiver};
use tokio::time::{sleep, Duration as TokioDuration};
use std::collections::{VecDeque, HashMap};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};
use std::borrow::Cow;
use chrono::{Local, Datelike};
use rayon::prelude::*;

use crate::constants::*;
use crate::metrics::Metrics;

// 최대 최근 응답 시간 저장 개수
const MAX_RECENT_RESPONSES: usize = 1000;

// 로그 배치 크기 및 플러시 간격
const LOG_BATCH_SIZE: usize = 100;
const LOG_FLUSH_INTERVAL_MS: u64 = 5000; // 5초마다 로그 플러시
const LOG_CHANNEL_SIZE: usize = 10000;    // 로그 채널 크기

/// 로그 메시지 타입
#[derive(Debug)]
pub enum LogMessage {
    RequestLog { content: String, host: String },
    RejectLog { content: String, host: String, ip: String },
    FlushLogs,
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
}

/// 로그 배치 구조체
#[derive(Default)]
struct LogBatch {
    entries: Vec<String>,
    size: usize,
}

impl LogBatch {
    fn new() -> Self {
        Self {
            entries: Vec::with_capacity(LOG_BATCH_SIZE),
            size: 0,
        }
    }
    
    fn add(&mut self, entry: String) {
        self.size += entry.len();
        self.entries.push(entry);
    }
    
    fn should_flush(&self) -> bool {
        self.entries.len() >= LOG_BATCH_SIZE || self.size >= BUFFER_SIZE_MEDIUM
    }
    
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    
    fn clear(&mut self) {
        self.entries.clear();
        self.size = 0;
    }
}

/// TLS 요청 로깅을 담당하는 구조체
pub struct RequestLogger {
    metrics: Arc<RwLock<ResponseMetrics>>,
    request_start_times: Arc<RwLock<HashMap<String, Instant>>>,
    log_sender: Option<Sender<LogMessage>>,
    global_metrics: Arc<Metrics>,
}

/// HTTP 요청 파싱 결과
#[derive(Debug)]
enum ParseResult {
    Complete {
        request_line: String,
        headers: Vec<String>,
        body: Option<String>,
        duration_ms: Option<u64>,
    },
    Incomplete,
    Invalid,
}

impl RequestLogger {
    /// 새 RequestLogger 인스턴스 생성
    pub fn new() -> Self {
        RequestLogger {
            metrics: Arc::new(RwLock::new(ResponseMetrics::new())),
            request_start_times: Arc::new(RwLock::new(HashMap::new())),
            log_sender: None,
            global_metrics: Metrics::new(),
        }
    }
    
    /// 로그 작성기 초기화
    pub async fn init(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 로그 디렉터리 생성
        tokio::fs::create_dir_all("logs").await?;
        
        // 채널 생성 - 크기 증가
        let (tx, rx) = mpsc::channel::<LogMessage>(LOG_CHANNEL_SIZE);
        self.log_sender = Some(tx.clone());
        
        // 로그 작성 태스크 시작
        tokio::spawn(Self::log_writer_task(rx));
        
        // 주기적 로그 플러시 태스크 시작
        tokio::spawn(async move {
            loop {
                sleep(TokioDuration::from_millis(LOG_FLUSH_INTERVAL_MS)).await;
                if tx.send(LogMessage::FlushLogs).await.is_err() {
                    break;
                }
            }
        });
        
        info!("RequestLogger initialized with async logging and batching");
        Ok(())
    }
    
    /// 로그 작성 태스크
    async fn log_writer_task(mut rx: Receiver<LogMessage>) {
        let mut file_cache: HashMap<String, File> = HashMap::new();
        
        // 로그 배치 맵
        let mut log_batches: HashMap<String, LogBatch> = HashMap::new();
        
        while let Some(message) = rx.recv().await {
            match message {
                LogMessage::RequestLog { content, host } => {
                    // 배치에 추가
                    let batch = log_batches.entry(host.clone()).or_insert_with(LogBatch::new);
                    batch.add(format!("{}\n\n", content));
                    
                    // 배치가 충분히 크면 플러시
                    if batch.should_flush() {
                        Self::flush_log_batch(&mut file_cache, &host, batch).await;
                    }
                },
                LogMessage::RejectLog { content, host, ip } => {
                    // 차단 로그 파일 이름 생성
                    let today = Local::now();
                    let date_str = format!("{:04}{:02}{:02}", 
                        today.year(), today.month(), today.day());
                    let log_key = format!("{}_{}_reject", host, date_str);
                    
                    // IP 정보와 함께 로그 작성
                    let log_content = format!("# IP: {}\n{}\n\n", ip, content);
                    
                    // 배치에 추가
                    let batch = log_batches.entry(log_key.clone()).or_insert_with(LogBatch::new);
                    batch.add(log_content);
                    
                    // 배치가 충분히 크면 플러시
                    if batch.should_flush() {
                        Self::flush_log_batch(&mut file_cache, &log_key, batch).await;
                    }
                },
                LogMessage::FlushLogs => {
                    // 모든 배치 플러시
                    for (host, batch) in log_batches.iter_mut() {
                        if !batch.is_empty() {
                            Self::flush_log_batch(&mut file_cache, host, batch).await;
                        }
                    }
                }
            }
        }
        
        // 채널이 닫히면 남은 배치 모두 플러시
        for (host, batch) in log_batches.iter_mut() {
            if !batch.is_empty() {
                Self::flush_log_batch(&mut file_cache, host, batch).await;
            }
        }
    }
    
    /// 로그 배치 플러시
    async fn flush_log_batch(file_cache: &mut HashMap<String, File>, host: &str, batch: &mut LogBatch) {
        if batch.is_empty() {
            return;
        }
        
        // 파일이 없으면 생성
        if !file_cache.contains_key(host) {
            let is_reject = host.contains("_reject");
            let log_path = if is_reject {
                format!("logs/request_{}.log", host.split('_').next().unwrap_or(host))
            } else {
                let today = Local::now();
                let date_str = format!("{:04}{:02}{:02}", today.year(), today.month(), today.day());
                format!("logs/request_{}_{}.log", host, date_str)
            };
            
            match tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_path)
                .await {
                    Ok(file) => {
                        file_cache.insert(host.to_string(), file);
                    },
                    Err(e) => {
                        error!("Failed to open log file for {}: {}", host, e);
                        batch.clear();
                        return;
                    }
                }
        }
        
        // 배치 내용을 병렬로 결합
        let combined_content = batch.entries.par_iter()
            .fold(String::new, |mut acc, entry| {
                acc.push_str(entry);
                acc
            })
            .reduce(String::new, |mut acc, partial| {
                acc.push_str(&partial);
                acc
            });
        
        // 파일에 로그 작성
        if let Some(file) = file_cache.get_mut(host) {
            if let Err(e) = file.write_all(combined_content.as_bytes()).await {
                error!("Failed to write to log file: {}", e);
            }
        }
        
        batch.clear();
    }
    
    /// 새로운 로그 파일 생성
    pub async fn create_log_file(host: &str, session_id: &str) -> Option<File> {
        // 로그 디렉터리 생성 확인
        if let Err(e) = tokio::fs::create_dir_all("logs").await {
            error!("[Session:{}] Failed to create logs directory: {}", session_id, e);
            return None;
        }
        
        let today = Local::now();
        let date_str = format!("{:04}{:02}{:02}", today.year(), today.month(), today.day());
        let request_log_path = format!("logs/request_{}_{}.log", host, date_str);
        
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
        if let Ok(mut start_times) = self.request_start_times.write() {
            start_times.insert(request_id.to_string(), Instant::now());
        }
    }
    
    /// 응답 완료 시간 기록 및 메트릭 업데이트
    pub fn record_request_end(&self, request_id: &str) -> Option<u64> {
        if let Ok(mut start_times) = self.request_start_times.write() {
            if let Some(start_time) = start_times.remove(request_id) {
                let duration = start_time.elapsed();
                let duration_ms = duration.as_millis() as u64;
                
                // 로컬 메트릭 업데이트
                if let Ok(mut metrics) = self.metrics.write() {
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
    pub fn log_async<'a>(&self, content: impl Into<Cow<'a, str>>, host: impl Into<Cow<'a, str>>) -> Result<(), &'static str> {
        if let Some(sender) = &self.log_sender {
            let message = LogMessage::RequestLog { 
                content: content.into().into_owned(), 
                host: host.into().into_owned() 
            };
            if sender.try_send(message).is_err() {
                return Err("로그 채널이 가득 찼습니다");
            }
            Ok(())
        } else {
            Err("로그 작성기가 초기화되지 않았습니다")
        }
    }
    
    /// 비동기로 차단 로그 작성
    pub fn log_reject_async<'a>(
        &self, 
        content: impl Into<Cow<'a, str>>, 
        host: impl Into<Cow<'a, str>>, 
        ip: impl Into<Cow<'a, str>>
    ) -> Result<(), &'static str> {
        if let Some(sender) = &self.log_sender {
            let message = LogMessage::RejectLog { 
                content: content.into().into_owned(), 
                host: host.into().into_owned(), 
                ip: ip.into().into_owned() 
            };
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
        
        // 버퍼가 너무 커지면 초기화 (메모리 누수 방지)
        if accumulated_data.len() > BUFFER_SIZE_LARGE {
            debug!("[Session:{}] Accumulated buffer too large, clearing", session_id);
            accumulated_data.clear();
            *processing_request = false;
            return;
        }
        
        // 데이터를 문자열로 변환
        let data_str = match std::str::from_utf8(accumulated_data) {
            Ok(s) => Cow::Borrowed(s),
            Err(_) => {
                debug!("[Session:{}] Invalid UTF-8 data in request", session_id);
                return;
            }
        };
        
        // 새 HTTP 요청 시작 감지
        let should_start_processing = !*processing_request && self.is_http_request_start(&data_str);
        
        if should_start_processing {
            *processing_request = true;
            self.record_request_start(request_id);
            debug!("[Session:{}] 새 HTTP 요청 감지: {}", 
                   session_id, data_str.lines().next().unwrap_or(""));
        }
        
        // HTTP 요청 처리
        if *processing_request {
            match self.parse_http_request(&data_str, request_id) {
                ParseResult::Complete { request_line, headers, body, duration_ms } => {
                    // 로그 내용 구성
                    let log_content = self.format_log_content(request_line, headers, body, duration_ms);
                    
                    // 로그 기록
                    self.write_log(log_content, file, host, session_id).await;
                    
                    // 처리 완료 후 상태 초기화
                    accumulated_data.clear();
                    *processing_request = false;
                },
                ParseResult::Incomplete => {
                    // 요청이 아직 완료되지 않음, 더 많은 데이터를 기다림
                },
                ParseResult::Invalid => {
                    // 유효하지 않은 요청, 버퍼 초기화
                    debug!("[Session:{}] Invalid HTTP request format", session_id);
                    accumulated_data.clear();
                    *processing_request = false;
                }
            }
        }
    }
    
    /// HTTP 요청 시작인지 확인
    fn is_http_request_start<'a>(&self, data: &'a str) -> bool {
        data.starts_with("GET ") || 
        data.starts_with("POST ") || 
        data.starts_with("PUT ") || 
        data.starts_with("DELETE ") || 
        data.starts_with("HEAD ") || 
        data.starts_with("OPTIONS ")
    }
    
    /// HTTP 요청 파싱
    fn parse_http_request<'a>(&self, data: &'a str, request_id: &str) -> ParseResult {
        // 헤더와 본문 분리 시도
        if !data.contains("\r\n\r\n") {
            return ParseResult::Incomplete;
        }
        
        let parts: Vec<&str> = data.split("\r\n\r\n").collect();
        let headers_section = parts[0];
        
        // 헤더 라인 분리
        let lines: Vec<&str> = headers_section.lines().collect();
        if lines.is_empty() {
            return ParseResult::Invalid;
        }
        
        // 첫 번째 줄은 요청 라인
        let request_line = lines[0].to_string();
        
        // 필요한 헤더 추출 - 병렬 처리로 개선
        let headers = lines[1..]
            .par_iter()
            .filter(|line| {
                let lower = line.to_lowercase();
                lower.starts_with("host:") ||
                lower.starts_with("user-agent:") ||
                lower.starts_with("referer:") ||
                lower.starts_with("sec-ch-ua-platform:") ||
                (request_line.starts_with("POST ") && (
                    lower.starts_with("content-length:") ||
                    lower.starts_with("content-type:")
                ))
            })
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        
        // POST 요청에 대한 본문 처리
        let body = if request_line.starts_with("POST ") && parts.len() > 1 {
            // Content-Length 값 추출
            let content_length = headers.iter()
                .filter(|h| h.to_lowercase().starts_with("content-length:"))
                .next()
                .and_then(|h| {
                    h.split(':')
                     .nth(1)
                     .and_then(|v| v.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            
            let body = parts[1];
            
            // 전체 본문을 받았거나 충분한 양을 받았는지 확인
            if content_length == 0 || body.len() >= content_length || body.len() >= 4096 {
                // 본문 추가 (최대 4096)
                let body_preview = if body.len() > 4096 {
                    format!("{}... (truncated)", &body[0..4096])
                } else {
                    body.to_string()
                };
                
                Some(body_preview)
            } else {
                // 아직 본문이 완전히 수신되지 않음
                return ParseResult::Incomplete;
            }
        } else {
            None
        };
        
        // 응답 시간 계산
        let duration_ms = self.record_request_end(request_id);
        
        ParseResult::Complete {
            request_line,
            headers,
            body,
            duration_ms,
        }
    }
    
    /// 로그 내용 형식화
    fn format_log_content(
        &self,
        request_line: String,
        headers: Vec<String>,
        body: Option<String>,
        duration_ms: Option<u64>,
    ) -> String {
        // 용량 예상해서 미리 할당
        let mut content = String::with_capacity(
            request_line.len() + 
            headers.iter().map(|h| h.len() + 1).sum::<usize>() + 
            body.as_ref().map_or(0, |b| b.len() + 2) +
            duration_ms.map_or(0, |_| 30)
        );
        
        // 요청 라인 추가
        content.push_str(&request_line);
        content.push('\n');
        
        // 헤더 추가
        for header in headers {
            content.push_str(&header);
            content.push('\n');
        }
        
        // 본문 추가 (있는 경우)
        if let Some(body_text) = body {
            content.push('\n'); // 헤더와 본문 사이 빈 줄
            content.push_str(&body_text);
        }
        
        // 응답 시간 추가 (있는 경우)
        if let Some(time) = duration_ms {
            content.push_str("\nResponse-Time: ");
            content.push_str(&time.to_string());
            content.push_str(" ms\n");
        }
        
        content
    }
    
    /// 로그 파일에 기록
    async fn write_log(&self, log_content: String, file: &mut Option<File>, host: &str, session_id: &str) {
        if let Some(file) = file {
            if let Err(e) = file.write_all(format!("{}\n\n", log_content).as_bytes()).await {
                error!("[Session:{}] Failed to write to request log file: {}", session_id, e);
            }
        } else {
            // 비동기 로깅 시도
            if let Err(e) = self.log_async(log_content, host) {
                error!("[Session:{}] Failed to log asynchronously: {}", session_id, e);
            }
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
            
            // 필요한 헤더 추출 - 병렬 처리로 개선
            let filtered_headers: Vec<String> = lines[1..]
                .par_iter()
                .filter(|line| {
                    let lower = line.to_lowercase();
                    lower.starts_with("host:") ||
                    lower.starts_with("user-agent:") ||
                    lower.starts_with("referer:") ||
                    lower.starts_with("sec-ch-ua-platform:")
                })
                .map(|&s| format!("{}\n", s))
                .collect();
                
            // 헤더 추가
            for header in filtered_headers {
                log_content.push_str(&header);
            }
            
            // 비동기 로깅 시도
            if let Err(e) = self.log_reject_async(log_content, host, ip) {
                error!("[Session:{}] Failed to log rejected request: {}", session_id, e);
            }
        }
    }
} 