use log::{debug, error, info, warn};
use tokio::sync::mpsc::{self, Sender, Receiver};
use tokio::time::{sleep, Duration as TokioDuration};
use std::borrow::Cow;
use std::error::Error;
use std::time::Instant;
use chrono::{DateTime, Utc, Datelike};

use crate::constants::*;
use crate::db;



/// 로그 메시지 타입
#[derive(Debug)]
pub enum LogMessage {
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
        response_time: Option<u64>,
        is_rejected: bool,
        is_tls: bool
    },
    ResponseTimeUpdate {
        session_id: String,
        response_time: u64,
    },
    FlushLogs,
}

/// 로그 배치 구조체
#[derive(Default)]
struct LogBatch {
    request_logs: Vec<(String, String, String, String, Option<String>, DateTime<Utc>, String, String, String, Option<u64>, bool, bool)>, // host, method, path, header, body, timestamp, session_id, client_ip, target_ip, response_time, is_rejected, is_tls
    size: usize,
}

/// 응답 시간 업데이트 배치 구조체
#[derive(Default)]
struct ResponseTimeBatch {
    updates: Vec<(String, u64)>, // (session_id, response_time)
    size: usize,
}

impl LogBatch {
    fn new() -> Self {
        Self {
            request_logs: Vec::with_capacity(LOG_BATCH_SIZE),
            size: 0,
        }
    }
    
    fn add_request(&mut self, host: String, method: String, path: String, header: String, body: Option<String>, timestamp: DateTime<Utc>, session_id: String, client_ip: String, target_ip: String, response_time: Option<u64>, is_rejected: bool, is_tls: bool) {
        // 크기 계산 (헤더와 바디 크기 합산)
        let content_size = header.len() + body.as_ref().map_or(0, |b| b.len());
        self.size += content_size;
        self.request_logs.push((host, method, path, header, body, timestamp, session_id, client_ip, target_ip, response_time, is_rejected, is_tls));
    }
    
    fn should_flush(&self) -> bool {
        self.request_logs.len() >= LOG_BATCH_SIZE || self.size >= BUFFER_SIZE_MEDIUM
    }
    
    fn is_empty(&self) -> bool {
        self.request_logs.is_empty()
    }
    
    fn clear(&mut self) {
        self.request_logs.clear();
        self.size = 0;
    }

    // 배치 쿼리 생성 메서드
    fn create_batch_query(&self) -> (String, Vec<Box<dyn tokio_postgres::types::ToSql + Send + Sync + 'static>>) {
        let mut query = "INSERT INTO request_logs (host, method, path, header, body, timestamp, session_id, client_ip, target_ip, response_time, is_rejected, is_tls) VALUES ".to_string();
        let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Send + Sync + 'static>> = Vec::new();
        
        for (i, (host, method, path, header, body, timestamp, session_id, client_ip, target_ip, response_time, is_rejected, is_tls)) in self.request_logs.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            
            let start_idx = i * 12 + 1;
            query.push_str(&format!("(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                start_idx, start_idx + 1, start_idx + 2, start_idx + 3, start_idx + 4,
                start_idx + 5, start_idx + 6, start_idx + 7, start_idx + 8, start_idx + 9, start_idx + 10, start_idx + 11));
            
            params.push(Box::new(host.clone()));
            params.push(Box::new(method.clone()));
            params.push(Box::new(path.clone()));
            params.push(Box::new(header.clone()));
            
            match body {
                Some(b) => params.push(Box::new(b.clone())),
                None => params.push(Box::new(Option::<String>::None)),
            }
            
            params.push(Box::new(*timestamp));
            params.push(Box::new(session_id.clone()));
            params.push(Box::new(client_ip.clone()));
            params.push(Box::new(target_ip.clone()));
            
            let rt_i64 = response_time.map(|rt| rt as i64);
            params.push(Box::new(rt_i64));
            params.push(Box::new(*is_rejected));
            params.push(Box::new(*is_tls));
        }
        
        (query, params)
    }
    
    // 단일 로그 항목에 대한 쿼리 생성
    fn create_single_query() -> String {
        "INSERT INTO request_logs (host, method, path, header, body, timestamp, session_id, client_ip, target_ip, response_time, is_rejected, is_tls) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)".to_string()
    }
    
    // 단일 로그 항목에 대한 파라미터 생성
    fn create_single_params(&self, index: usize) -> Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send + '_>> {
        if let Some((host, method, path, header, body, timestamp, session_id, client_ip, target_ip, response_time, is_rejected, is_tls)) = self.request_logs.get(index) {
            let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send + '_>> = Vec::with_capacity(12);
            
            params.push(Box::new(host.clone()));
            params.push(Box::new(method.clone()));
            params.push(Box::new(path.clone()));
            params.push(Box::new(header.clone()));
            params.push(Box::new(body.clone()));
            params.push(Box::new(*timestamp));
            params.push(Box::new(session_id.clone()));
            params.push(Box::new(client_ip.clone()));
            params.push(Box::new(target_ip.clone()));
            
            // 응답 시간은 Option<u64>를 Option<i64>로 변환
            let rt_i64 = response_time.map(|rt| rt as i64);
            params.push(Box::new(rt_i64));
            
            params.push(Box::new(*is_rejected));
            params.push(Box::new(*is_tls));
            
            params
        } else {
            vec![]
        }
    }
}

impl ResponseTimeBatch {
    fn new() -> Self {
        Self {
            updates: Vec::with_capacity(LOG_BATCH_SIZE),
            size: 0,
        }
    }
    
    fn add_update(&mut self, session_id: String, response_time: u64) {
        self.updates.push((session_id, response_time));
        self.size += 1;
    }
    
    fn should_flush(&self) -> bool {
        self.updates.len() >= LOG_BATCH_SIZE / 2 // 요청 로그의 절반 크기로 설정
    }
    
    fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }
    
    fn clear(&mut self) {
        self.updates.clear();
        self.size = 0;
    }
    
    // 배치 업데이트 쿼리 생성
    fn create_batch_query(&self) -> (String, Vec<String>) {
        let mut session_ids = Vec::with_capacity(self.updates.len());
        
        // CASE 문을 사용한 업데이트 쿼리 생성
        let mut query = "
            UPDATE request_logs
            SET response_time = CASE session_id ".to_string();
            
        for (session_id, response_time) in &self.updates {
            query.push_str(&format!("WHEN '{}' THEN {} ", session_id, response_time));
            session_ids.push(session_id.clone());
        }
        
        query.push_str("END WHERE session_id IN (");
        
        for (i, session_id) in session_ids.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(&format!("'{}'", session_id));
        }
        
        query.push_str(") AND timestamp > NOW() - INTERVAL '3 hour' AND response_time IS NULL");
        
        (query, session_ids)
    }
}

/// TLS 요청 로깅을 담당하는 구조체
#[derive(Clone)]
pub struct RequestLogger {
    log_sender: Option<Sender<LogMessage>>,
}

impl RequestLogger {
    /// 새 RequestLogger 인스턴스 생성
    pub fn new() -> Self {
        RequestLogger {
            log_sender: None,
        }
    }
    
    /// 로그 작성기 초기화
    pub async fn init(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        debug!("RequestLogger 초기화 시작...");
        
        // 파티션 관리 확인
        debug!("데이터베이스 파티션 확인 중...");
        match db::ensure_partitions().await {
            Ok(_) => debug!("데이터베이스 파티션 확인 완료"),
            Err(e) => {
                warn!("데이터베이스 파티션 확인 실패: {}. 계속 진행합니다.", e);
                // 파티션 확인 실패해도 계속 진행
            }
        }

        // 직접 테이블 생성 시도 (파티션 관리가 실패했을 경우를 대비)
        debug!("기본 테이블 존재 여부 확인 중...");
        if let Err(e) = Self::ensure_base_tables().await {
            error!("기본 테이블 생성 실패: {}", e);
            return Err(e);
        }
        
        // 채널 생성 - 크기 증가
        debug!("로그 채널 생성 중... (크기: {})", LOG_CHANNEL_SIZE);
        let (tx, rx) = mpsc::channel::<LogMessage>(LOG_CHANNEL_SIZE);
        self.log_sender = Some(tx.clone());
        
        // 로그 작성 태스크 시작
        debug!("로그 작성 태스크 시작...");
        tokio::spawn(Self::log_writer_task(rx));
        
        // 주기적 로그 플러시 태스크 시작
        debug!("주기적 로그 플러시 태스크 시작...");
        tokio::spawn(async move {
            loop {
                sleep(TokioDuration::from_millis(LOG_FLUSH_INTERVAL_MS)).await;
                match tx.send(LogMessage::FlushLogs).await {
                    Ok(_) => debug!("로그 플러시 명령 전송 성공"),
                    Err(e) => {
                        error!("로그 플러시 명령 전송 실패: {}", e);
                        break;
                    }
                }
            }
        });
        
        info!("RequestLogger 초기화 완료: PostgreSQL 로깅 및 배치 처리 활성화됨");
        Ok(())
    }
    
    /// 로그 작성 태스크
    async fn log_writer_task(mut rx: Receiver<LogMessage>) {
        let mut log_batch = LogBatch::new();
        let mut response_time_batch = ResponseTimeBatch::new();
        let mut last_flush_time = Instant::now();
        
        // 배치 플러시 주기
        let flush_interval = TokioDuration::from_millis(LOG_FLUSH_INTERVAL_MS);
        
        while let Some(message) = rx.recv().await {
            match message {
                LogMessage::RequestLog { host, method, path, header, body, timestamp, session_id, client_ip, target_ip, response_time, is_rejected, is_tls } => {
                    log_batch.add_request(host, method, path, header, body, timestamp, session_id, client_ip, target_ip, response_time, is_rejected, is_tls);
                    
                    if log_batch.should_flush() {
                        if let Err(e) = Self::flush_log_batch(&mut log_batch).await {
                            error!("로그 배치 플러시 실패: {}", e);
                        }
                        last_flush_time = Instant::now();
                    }
                },
                LogMessage::ResponseTimeUpdate { session_id, response_time } => {
                    debug!("[Session:{}] 응답 시간 업데이트 배치에 추가: {}ms", session_id, response_time);
                    response_time_batch.add_update(session_id, response_time);
                    
                    if response_time_batch.should_flush() {
                        if let Err(e) = Self::flush_response_time_batch(&mut response_time_batch).await {
                            error!("응답 시간 배치 플러시 실패: {}", e);
                        }
                    }
                },
                LogMessage::FlushLogs => {
                    // 로그 배치 플러시
                    if !log_batch.is_empty() {
                        if let Err(e) = Self::flush_log_batch(&mut log_batch).await {
                            error!("로그 배치 플러시 실패: {}", e);
                        }
                    }
                    
                    // 응답 시간 배치 플러시
                    if !response_time_batch.is_empty() {
                        if let Err(e) = Self::flush_response_time_batch(&mut response_time_batch).await {
                            error!("응답 시간 배치 플러시 실패: {}", e);
                        }
                    }
                    
                    last_flush_time = Instant::now();
                }
            }
            
            // 일정 시간이 지나면 배치 플러시
            if last_flush_time.elapsed() >= flush_interval {
                // 로그 배치 플러시
                if !log_batch.is_empty() {
                    if let Err(e) = Self::flush_log_batch(&mut log_batch).await {
                        error!("로그 배치 플러시 실패: {}", e);
                    }
                }
                
                // 응답 시간 배치 플러시
                if !response_time_batch.is_empty() {
                    if let Err(e) = Self::flush_response_time_batch(&mut response_time_batch).await {
                        error!("응답 시간 배치 플러시 실패: {}", e);
                    }
                }
                
                last_flush_time = Instant::now();
            }
        }
    }
    
    /// 로그 배치 플러시
    async fn flush_log_batch(batch: &mut LogBatch) -> Result<(), Box<dyn Error + Send + Sync>> {
        if batch.is_empty() {
            return Ok(());
        }
        
        debug!("로그 배치 플러시 중: {} 항목", batch.request_logs.len());
        
        // 쿼리 실행기 가져오기
        let executor = match db::query::QueryExecutor::get_instance().await {
            Ok(executor) => executor,
            Err(e) => {
                error!("쿼리 실행기 가져오기 실패: {}", e);
                return Err(e.into());
            }
        };
        
        // 배치 처리 시도
        let (_query, _params) = batch.create_batch_query();
        
        // 요청 로그 플러시
        if !batch.request_logs.is_empty() {
            debug!("배치 쿼리로 {} 개의 로그 삽입 시도", batch.request_logs.len());
            
            // 배치 쿼리 및 파라미터 생성
            let (_query, _params) = batch.create_batch_query();
            
            // 파라미터 참조 벡터 생성
            let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = _params.iter()
                .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();
            
            // 배치 쿼리 실행
            match executor.execute_query(&_query, &param_refs).await {
                Ok(rows) => {
                    debug!("배치 쿼리로 {} 개의 로그 삽입 성공", rows);
                },
                Err(e) => {
                    let error_msg = e.to_string();
                    // 파티션 관련 오류인지 확인
                    if error_msg.contains("no partition") || error_msg.contains("partition") {
                        // 파티션 관련 오류 발생 시 개별 삽입으로 폴백
                        error!("배치 삽입 중 파티션 관련 오류 발생: {}. 개별 삽입으로 전환합니다.", error_msg);
                        
                        // 개별 삽입 쿼리 생성
                        let single_query = LogBatch::create_single_query();
                        
                        // 각 로그 항목을 개별적으로 삽입
                        for i in 0..batch.request_logs.len() {
                            let boxed_params = batch.create_single_params(i);
                            
                            // Box<dyn ToSql + Sync>의 참조를 &(dyn ToSql + Sync)로 변환
                            let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = boxed_params.iter()
                                .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
                                .collect();
                            
                            // 쿼리 실행
                            if let Err(e) = executor.execute_query(&single_query, &param_refs).await {
                                let error_msg = e.to_string();
                                // 파티션 관련 오류인지 확인
                                if error_msg.contains("no partition") || error_msg.contains("partition") {
                                    // 날짜 파티션이 존재하지 않는 경우 파티션 생성 시도
                                    error!("파티션 관련 오류 발생: {}", error_msg);
                                    
                                    if let Some((_, _, _, _, _, timestamp, _, _, _, _, _, _)) = batch.request_logs.get(i) {
                                        // 타임스탬프 파티션을 위한 날짜 추출
                                        let date = timestamp.date_naive();
                                        let next_date = date + chrono::Duration::days(1);
                                        
                                        // 파티션 이름 생성
                                        let partition_name = format!("request_logs_{:04}{:02}{:02}", 
                                            date.year(), date.month(), date.day());
                                        
                                        let create_partition_query = format!(
                                            "CREATE TABLE IF NOT EXISTS {} PARTITION OF request_logs 
                                            FOR VALUES FROM ('{}') TO ('{}')",
                                            partition_name, date, next_date
                                        );
                                        
                                        match executor.execute_query(&create_partition_query, &[]).await {
                                            Ok(_) => {
                                                info!("누락된 파티션 자동 생성: {}", partition_name);
                                                
                                                // 파티션 생성 후 다시 로그 저장 시도
                                                if let Err(e) = executor.execute_query(&single_query, &param_refs).await {
                                                    error!("파티션 생성 후에도 요청 로그 삽입 실패: {}", e);
                                                }
                                            },
                                            Err(e) => {
                                                error!("누락된 파티션 생성 실패: {}", e);
                                            }
                                        }
                                    }
                                } else {
                                    error!("요청 로그 삽입 실패: {}", e);
                                }
                            }
                        }
                    } else {
                        error!("배치 쿼리로 로그 삽입 실패: {}. 데이터가 손실될 수 있습니다.", e);
                    }
                }
            }
            
            debug!("로그 배치 처리 완료");
        }
        
        batch.clear();
        Ok(())
    }
    
    /// 응답 시간 배치 플러시
    async fn flush_response_time_batch(batch: &mut ResponseTimeBatch) -> Result<(), Box<dyn Error + Send + Sync>> {
        if batch.is_empty() {
            return Ok(());
        }
        
        debug!("응답 시간 배치 플러시 중: {} 항목", batch.updates.len());
        
        // 쿼리 실행기 가져오기
        let executor = match db::query::QueryExecutor::get_instance().await {
            Ok(executor) => executor,
            Err(e) => {
                error!("쿼리 실행기 가져오기 실패: {}", e);
                return Err(e.into());
            }
        };
        
        // 배치 쿼리 생성
        let (query, _session_ids) = batch.create_batch_query();
        
        // 쿼리 실행
        match executor.execute_query(&query, &[]).await {
            Ok(rows) => {
                debug!("응답 시간 배치 업데이트 성공: {} 개의 세션 중 {} 개 업데이트됨", 
                      batch.updates.len(), rows);
                
                // 업데이트된 행이 없는 경우 개별 업데이트 시도
                if rows == 0 && !batch.updates.is_empty() {
                    debug!("배치 업데이트에 실패한 세션에 대해 개별 업데이트 시도");
                    
                    // 최대 10개까지만 개별 업데이트 시도 (성능 이슈 방지)
                    let max_individual_attempts = 10.min(batch.updates.len());
                    
                    for i in 0..max_individual_attempts {
                        let (session_id, response_time) = &batch.updates[i];
                        
                        // 개별 업데이트 쿼리 - 더 넓은 시간 범위로 수정
                        let individual_query = "
                            UPDATE request_logs 
                            SET response_time = $1 
                            WHERE id = (
                                SELECT id 
                                FROM request_logs 
                                WHERE session_id = $2 
                                AND timestamp > NOW() - INTERVAL '24 hour'
                                AND response_time IS NULL
                                ORDER BY timestamp DESC
                                LIMIT 1
                            )";
                        
                        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
                            &(*response_time as i64),
                            &session_id
                        ];
                        
                        match executor.execute_query(individual_query, params).await {
                            Ok(updated) => {
                                if updated > 0 {
                                    debug!("[Session:{}] 개별 응답 시간 업데이트 성공: {}ms", session_id, response_time);
                                } else {
                                    // 업데이트된 행이 없는 경우 더 넓은 시간 범위로 다시 시도
                                    let wider_query = "
                                        UPDATE request_logs 
                                        SET response_time = $1 
                                        WHERE id = (
                                            SELECT id 
                                            FROM request_logs 
                                            WHERE session_id = $2 
                                            AND response_time IS NULL
                                            ORDER BY timestamp DESC
                                            LIMIT 1
                                        )";
                                    
                                    match executor.execute_query(wider_query, params).await {
                                        Ok(wider_updated) => {
                                            if wider_updated > 0 {
                                                debug!("[Session:{}] 넓은 범위 응답 시간 업데이트 성공: {}ms", session_id, response_time);
                                            } else {
                                                debug!("[Session:{}] 응답 시간 업데이트 실패: 매칭되는 요청 레코드 없음", session_id);
                                            }
                                        },
                                        Err(e) => {
                                            debug!("[Session:{}] 넓은 범위 응답 시간 업데이트 실패: {}", session_id, e);
                                        }
                                    }
                                }
                            },
                            Err(e) => {
                                debug!("[Session:{}] 개별 응답 시간 업데이트 실패: {}", session_id, e);
                            }
                        }
                    }
                }
            },
            Err(e) => {
                error!("응답 시간 배치 업데이트 실패: {}", e);
            }
        }
        
        batch.clear();
        Ok(())
    }
    
    /// 비동기 로그 기록
    pub fn log_async<'a>(
        &self, 
        host: impl Into<Cow<'a, str>>, 
        method: impl Into<Cow<'a, str>>,
        path: impl Into<Cow<'a, str>>,
        header: impl Into<Cow<'a, str>>,
        body: Option<String>,
        session_id: impl Into<Cow<'a, str>>,
        client_ip: impl Into<Cow<'a, str>>,
        target_ip: impl Into<Cow<'a, str>>,
        response_time: Option<u64>,
        is_rejected: bool,
        is_tls: bool
    ) -> Result<(), &'static str> {
        let host_str = host.into().to_string();
        let method_str = method.into().to_string();
        let path_str = path.into().to_string();
        let header_str = header.into().to_string();
        let session_id_str = session_id.into().to_string();
        let client_ip_str = client_ip.into().to_string();
        let target_ip_str = target_ip.into().to_string();
        
        match &self.log_sender {
            Some(sender) => {
                let timestamp = Utc::now();
                let session_id_clone = session_id_str.clone(); // session_id_str 복제
                
                if sender.try_send(LogMessage::RequestLog { 
                    host: host_str, 
                    method: method_str,
                    path: path_str,
                    header: header_str,
                    body,
                    timestamp, 
                    session_id: session_id_str,
                    client_ip: client_ip_str,
                    target_ip: target_ip_str,
                    response_time,
                    is_rejected,
                    is_tls
                }).is_err() {
                    warn!("[Session:{}] 로그 채널이 가득 찼습니다", session_id_clone);
                    return Err("로그 채널이 가득 찼습니다");
                }
                Ok(())
            },
            None => {
                // 로그 채널이 초기화되지 않았을 때 경고만 출력하고 계속 진행
                warn!("[Session:{}] 로그 채널이 초기화되지 않았습니다. 로그를 기록할 수 없습니다.", session_id_str);
                Ok(())
            }
        }
    }
    
    /// 차단된 요청 로깅
    pub async fn log_rejected_request(&self, request: &str, host: &str, ip: &str, session_id: &str, is_tls: bool) {
        let (method, path, header, body) = self.parse_request_for_reject(request);
        
        if let Some(sender) = &self.log_sender {
            let log_message = LogMessage::RequestLog {
                host: host.to_string(),
                client_ip: ip.to_string(),
                method,
                path,
                header,
                body,
                timestamp: Utc::now(),
                session_id: session_id.to_string(),
                target_ip: "Blocked".to_string(), // 차단된 요청은 타겟 IP를 "Blocked"로 표시
                response_time: None,
                is_rejected: true,
                is_tls // 차단된 요청의 TLS 여부
            };
            
            if let Err(e) = sender.send(log_message).await {
                error!("차단 로그 전송 실패: {}", e);
            }
        }
    }
    
    /// 응답 시간 업데이트 - 배치 처리 지원
    pub async fn update_response_time(&self, session_id: &str, response_time: u64) -> Result<(), Box<dyn Error + Send + Sync>> {
        match &self.log_sender {
            Some(sender) => {
                // 응답 시간 업데이트 메시지를 채널로 전송
                let message = LogMessage::ResponseTimeUpdate {
                    session_id: session_id.to_string(),
                    response_time,
                };
                
                // 메시지 전송 시도
                match sender.try_send(message) {
                    Ok(_) => {
                        debug!("[Session:{}] 응답 시간 업데이트 메시지 전송 성공: {}ms", session_id, response_time);
                        Ok(())
                    },
                    Err(e) => {
                        // 채널이 가득 찬 경우 직접 업데이트 시도
                        match e {
                            tokio::sync::mpsc::error::TrySendError::Full(_) => {
                                warn!("[Session:{}] 응답 시간 업데이트 채널이 가득 참, 직접 업데이트 시도: {}ms", 
                                     session_id, response_time);
                                self.update_response_time_direct(session_id, response_time).await
                            },
                            _ => {
                                error!("[Session:{}] 응답 시간 업데이트 메시지 전송 실패: {}", session_id, e);
                                Err(Box::new(std::io::Error::new(
                                    std::io::ErrorKind::Other, 
                                    format!("응답 시간 업데이트 메시지 전송 실패: {}", e)
                                )))
                            }
                        }
                    }
                }
            },
            None => {
                // 로그 채널이 초기화되지 않은 경우 직접 업데이트
                warn!("[Session:{}] 로그 채널이 초기화되지 않음, 직접 응답 시간 업데이트 시도: {}ms", 
                     session_id, response_time);
                self.update_response_time_direct(session_id, response_time).await
            }
        }
    }
    
    /// 응답 시간 직접 업데이트 (채널 사용 불가 시 대체 방법)
    async fn update_response_time_direct(&self, session_id: &str, response_time: u64) -> Result<(), Box<dyn Error + Send + Sync>> {
        // DB 연결 획득
        let executor = match db::query::QueryExecutor::get_instance().await {
            Ok(executor) => executor,
            Err(e) => {
                error!("응답 시간 직접 업데이트를 위한 쿼리 실행기 가져오기 실패: {}", e);
                return Err(e.into());
            }
        };
        
        // 업데이트 쿼리 실행 - 최근 요청 우선으로 검색
        let query = "
            UPDATE request_logs 
            SET response_time = $1 
            WHERE id = (
                SELECT id 
                FROM request_logs 
                WHERE session_id = $2 
                AND response_time IS NULL
                ORDER BY timestamp DESC
                LIMIT 1
            )";
        
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
            &(response_time as i64),
            &session_id
        ];
        
        // 최대 2번까지만 재시도 (직접 업데이트는 빠르게 결정)
        let mut attempts = 0;
        let max_attempts = 2;
        
        while attempts < max_attempts {
            match executor.execute_query(query, params).await {
                Ok(rows) => {
                    if rows > 0 {
                        info!("[Session:{}] 응답 시간 직접 업데이트 성공: {}ms", session_id, response_time);
                        return Ok(());
                    }
                    
                    // 업데이트된 행이 없으면 더 넓은 시간 범위로 다시 시도
                    if attempts == max_attempts - 1 {
                        // 세션 ID로만 검색하여 가장 최근 요청에 응답 시간 업데이트
                        debug!("[Session:{}] 기본 쿼리로 응답 시간 업데이트 실패, 확장 쿼리 시도", session_id);
                        
                        // 최근 24시간 내 요청 중 가장 최근 요청 찾기
                        let find_query = "
                            SELECT id, timestamp 
                            FROM request_logs 
                            WHERE session_id = $1 
                            AND response_time IS NULL
                            ORDER BY timestamp DESC
                            LIMIT 3";
                        
                        let find_params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&session_id];
                        
                        match executor.query_rows(find_query, find_params, |row| {
                            let id: i64 = row.get(0);
                            let timestamp: chrono::DateTime<chrono::Utc> = row.get(1);
                            Ok((id, timestamp))
                        }).await {
                            Ok(results) => {
                                if !results.is_empty() {
                                    let (id, timestamp) = &results[0];
                                    debug!("[Session:{}] 응답 시간 업데이트할 요청 찾음: ID={}, 시간={}", 
                                          session_id, id, timestamp);
                                    
                                    // 특정 ID로 직접 업데이트
                                    let direct_query = "UPDATE request_logs SET response_time = $1 WHERE id = $2";
                                    let direct_params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
                                        &(response_time as i64),
                                        id
                                    ];
                                    
                                    match executor.execute_query(direct_query, direct_params).await {
                                        Ok(updated) => {
                                            if updated > 0 {
                                                info!("[Session:{}] ID 기반 응답 시간 업데이트 성공: ID={}, 시간={}ms", 
                                                     session_id, id, response_time);
                                                return Ok(());
                                            } else {
                                                warn!("[Session:{}] ID 기반 응답 시간 업데이트 실패: ID={}", session_id, id);
                                            }
                                        },
                                        Err(e) => {
                                            error!("[Session:{}] ID 기반 응답 시간 업데이트 오류: {}", session_id, e);
                                        }
                                    }
                                } else {
                                    debug!("[Session:{}] 응답 시간을 업데이트할 요청을 찾을 수 없음", session_id);
                                }
                            },
                            Err(e) => {
                                error!("[Session:{}] 응답 시간 업데이트할 요청 검색 실패: {}", session_id, e);
                            }
                        }
                    }
                },
                Err(e) => {
                    error!("[Session:{}] 응답 시간 직접 업데이트 실패: {}", session_id, e);
                    if attempts == max_attempts - 1 {
                        return Err(e.into());
                    }
                }
            }
            
            // 재시도 전 짧은 지연
            if attempts < max_attempts - 1 {
                tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
            }
            
            attempts += 1;
        }
        
        // 모든 시도 실패했지만 세션 진행에 영향을 주지 않도록 에러를 반환하지 않음
        debug!("[Session:{}] 응답 시간 직접 업데이트 실패 (레코드를 찾을 수 없음)", session_id);
        Ok(())
    }
    
    /// 차단 로깅을 위한 요청 파싱
    fn parse_request_for_reject(&self, request: &str) -> (String, String, String, Option<String>) {
        let lines: Vec<&str> = request.split("\r\n").collect();
        if lines.is_empty() {
            return ("UNKNOWN".to_string(), "/".to_string(), "".to_string(), None);
        }
        
        // 요청 라인 파싱
        let request_line_parts: Vec<&str> = lines[0].split_whitespace().collect();
        let method = if request_line_parts.len() > 0 {
            request_line_parts[0].to_string()
        } else {
            "UNKNOWN".to_string()
        };
        
        let path = if request_line_parts.len() > 1 {
            request_line_parts[1].to_string()
        } else {
            "/".to_string()
        };
        
        // 헤더 파싱
        let mut header_str = String::new();
        let mut i = 1;
        while i < lines.len() && !lines[i].is_empty() {
            if !header_str.is_empty() {
                header_str.push_str("\r\n");
            }
            header_str.push_str(lines[i]);
            i += 1;
        }
        
        // 본문 파싱 - 메서드에 따라 다르게 처리
        let mut body = None;
        if method == "POST" || method == "PUT" || method == "PATCH" {
            if i < lines.len() - 1 {
                let body_start = i + 1;
                let body_content = lines[body_start..].join("\r\n");
                if !body_content.is_empty() {
                    body = Some(body_content);
                }
            }
        }
        
        (method, path, header_str, body)
    }
    
    /// 기본 테이블 생성을 확인하고 필요시 생성하는 메서드
    async fn ensure_base_tables() -> Result<(), Box<dyn Error + Send + Sync>> {
        debug!("기본 로깅 테이블 확인 중...");
        
        // 쿼리 실행기 가져오기
        let executor = match db::query::QueryExecutor::get_instance().await {
            Ok(executor) => executor,
            Err(e) => {
                error!("쿼리 실행기 가져오기 실패: {}", e);
                return Err(e.into());
            }
        };
        
        // 요청 로그 테이블 존재 여부 확인
        let check_table_query = "
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'request_logs'
            );
        ";
        
        let table_exists = match executor.query_one(check_table_query, &[], |row| Ok(row.get::<_, bool>(0))).await {
            Ok(exists) => exists,
            Err(e) => {
                error!("테이블 존재 여부 확인 실패: {}", e);
                false
            }
        };
        
        if !table_exists {
            // 테이블이 존재하지 않으면 새로 생성
            debug!("request_logs 테이블이 존재하지 않습니다. 새로 생성합니다.");
            
            // 요청 로그 테이블 생성
            let request_logs_query = "
                CREATE TABLE IF NOT EXISTS request_logs (
                    id SERIAL,
                    host TEXT NOT NULL,
                    method TEXT NOT NULL,
                    path TEXT NOT NULL,
                    header TEXT NOT NULL,
                    body TEXT,
                    timestamp TIMESTAMPTZ NOT NULL,
                    session_id TEXT NOT NULL,
                    client_ip TEXT NOT NULL,
                    target_ip TEXT NOT NULL,
                    response_time BIGINT,
                    is_rejected BOOLEAN NOT NULL DEFAULT FALSE,
                    is_tls BOOLEAN NOT NULL DEFAULT FALSE,
                    PRIMARY KEY (id, timestamp)
                ) PARTITION BY RANGE (timestamp);
            ";
            
            // 테이블 생성 실행
            if let Err(e) = executor.execute_query(request_logs_query, &[]).await {
                error!("request_logs 테이블 생성 실패: {}", e);
                return Err(e.into());
            }
            
            info!("request_logs 테이블이 성공적으로 생성되었습니다.");
        } 
        
        Ok(())
    }
} 