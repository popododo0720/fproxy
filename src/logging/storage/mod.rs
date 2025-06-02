use std::error::Error;
use std::time::Duration;
use chrono::{DateTime, Utc};
use log::{debug, error, info, warn};
use tokio::time::timeout;
use tokio_postgres::{Transaction, types::{Type, ToSql}};

use crate::constants::{
    request_logs, response_logs, LOG_BATCH_SIZE, LOG_DB_OPERATION_TIMEOUT_MS
};
use crate::db::query::QueryExecutor;
use deadpool_postgres::GenericClient;

mod batch;
pub use batch::*;

/// 로그 저장소 - DB 저장 담당
pub struct LogStorage {
    request_batch: ThreadSafeRequestBatch,
    response_batch: ThreadSafeResponseBatch,
    initialized: bool,
}

impl LogStorage {
    /// 새 로그 저장소 인스턴스 생성
    pub fn new() -> Self {
        Self {
            request_batch: new_request_batch(),
            response_batch: new_response_batch(),
            initialized: false,
        }
    }
    
    /// 로그 저장소 초기화 - 테이블 생성
    pub async fn init(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if self.initialized {
            return Ok(());
        }
        
        // 요청 로그 테이블 생성
        self.ensure_request_log_table().await?;
        
        // 응답 로그 테이블 생성
        self.ensure_response_log_table().await?;
        
        self.initialized = true;
        Ok(())
    }
    
    /// 요청 로그 테이블 생성
    async fn ensure_request_log_table(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let executor = QueryExecutor::get_instance().await?;
        
        // 테이블 존재 여부 확인
        let exists = executor.query_one(
            "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'request_logs')",
            &[],
            |row| Ok(row.get::<_, bool>(0))
        ).await?;
        
        if !exists {
            info!("request_logs 테이블이 존재하지 않습니다. 새로 생성합니다.");
            
            // 테이블 생성
            executor.execute_query(request_logs::CREATE_TABLE, &[]).await?;
            
            // 인덱스 생성
            for index_query in request_logs::CREATE_INDICES.iter() {
                if let Err(e) = executor.execute_query(index_query, &[]).await {
                    warn!("request_logs 인덱스 생성 실패: {}", e);
                }
            }
            
            info!("request_logs 테이블 생성 완료");
        }
        
        Ok(())
    }
    
    /// 응답 로그 테이블 생성
    async fn ensure_response_log_table(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let executor = QueryExecutor::get_instance().await?;
        
        // 테이블 존재 여부 확인
        let exists = executor.query_one(
            response_logs::CHECK_TABLE_EXISTS,
            &[],
            |row| Ok(row.get::<_, bool>(0))
        ).await?;
        
        if !exists {
            info!("response_logs 테이블이 존재하지 않습니다. 새로 생성합니다.");
            
            // 테이블 생성
            executor.execute_query(response_logs::CREATE_TABLE, &[]).await?;
            
            // 인덱스 생성
            for index_query in response_logs::CREATE_INDICES.iter() {
                if let Err(e) = executor.execute_query(index_query, &[]).await {
                    warn!("response_logs 인덱스 생성 실패: {}", e);
                }
            }
            
            info!("response_logs 테이블 생성 완료");
        }
        
        Ok(())
    }
    
    /// 요청 로그 추가
    pub fn add_request_log(
        &self,
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
        is_tls: bool
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 값 미리 복사
        let host_clone = host.clone();
        let method_clone = method.clone();
        let path_clone = path.clone();
        let header_clone = header.clone();
        let body_clone = body.clone();
        let timestamp_clone = timestamp;
        let session_id_clone = session_id.clone();
        let client_ip_clone = client_ip.clone();
        let target_ip_clone = target_ip.clone();
        let is_rejected_clone = is_rejected;
        let is_tls_clone = is_tls;
        
        // 배치에 로그 추가
        let batch = self.request_batch.clone();
        tokio::spawn(async move {
            let mut batch_guard = match batch.lock().await {
                guard => guard,
            };
            
            // 로그 추가 (이미 복사된 값 사용)
            batch_guard.add_log(
                host_clone, method_clone, path_clone, header_clone, body_clone, timestamp_clone, 
                session_id_clone, client_ip_clone, target_ip_clone, is_rejected_clone, is_tls_clone
            );
        });
        
        Ok(())
    }
    
    /// 응답 로그 추가
    pub fn add_response_log(
        &self,
        session_id: String,
        status_code: u16,
        response_time: u64,
        response_size: usize,
        timestamp: DateTime<Utc>,
        headers: String,
        body_preview: Option<String>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 값 미리 복사
        let session_id_clone = session_id.clone();
        let status_code_clone = status_code;
        let response_time_clone = response_time;
        let response_size_clone = response_size;
        let timestamp_clone = timestamp;
        let headers_clone = headers.clone();
        let body_preview_clone = body_preview.clone();
        
        // 배치에 로그 추가
        let batch = self.response_batch.clone();
        tokio::spawn(async move {
            let mut batch_guard = match batch.lock().await {
                guard => guard,
            };
            
            // 로그 추가 (이미 복사된 값 사용)
            batch_guard.add_log(
                session_id_clone, status_code_clone, response_time_clone, response_size_clone, 
                timestamp_clone, headers_clone, body_preview_clone
            );
        });
        
        Ok(())
    }
    
    /// 요청 로그 배치 플러시
    pub async fn flush_request_logs(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 배치 가져오기
        let mut batch = self.request_batch.lock().await;
        
        // 배치가 비어있으면 아무것도 하지 않음
        if batch.is_empty() {
            return Ok(());
        }
        
        // 배치 복사 및 비우기
        let logs = std::mem::take(&mut batch.logs);
        let log_count = logs.len();
        batch.clear();
        
        // 락 해제
        drop(batch);
        
        // 타임아웃과 함께 DB에 저장
        let result = timeout(
            Duration::from_millis(LOG_DB_OPERATION_TIMEOUT_MS),
            self.save_request_logs(logs)
        ).await;
        
        match result {
            Ok(Ok(_)) => {
                debug!("{} 개의 요청 로그 저장 완료", log_count);
                Ok(())
            },
            Ok(Err(e)) => {
                error!("요청 로그 저장 실패: {}", e);
                Err(e)
            },
            Err(_) => {
                error!("요청 로그 저장 타임아웃");
                Err("요청 로그 저장 타임아웃".into())
            }
        }
    }
    
    /// 응답 로그 배치 플러시
    pub async fn flush_response_logs(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 배치 가져오기
        let mut batch = self.response_batch.lock().await;
        
        // 배치가 비어있으면 아무것도 하지 않음
        if batch.is_empty() {
            return Ok(());
        }
        
        // 배치 복사 및 비우기
        let logs = std::mem::take(&mut batch.logs);
        let log_count = logs.len();
        batch.clear();
        
        // 락 해제
        drop(batch);
        
        // 타임아웃과 함께 DB에 저장
        let result = timeout(
            Duration::from_millis(LOG_DB_OPERATION_TIMEOUT_MS),
            self.save_response_logs(logs)
        ).await;
        
        match result {
            Ok(Ok(_)) => {
                debug!("{} 개의 응답 로그 저장 완료", log_count);
                Ok(())
            },
            Ok(Err(e)) => {
                error!("응답 로그 저장 실패: {}", e);
                Err(e)
            },
            Err(_) => {
                error!("응답 로그 저장 타임아웃");
                Err("응답 로그 저장 타임아웃".into())
            }
        }
    }
    
    /// 요청 로그 저장
    async fn save_request_logs(
        &self, 
        logs: Vec<(String, String, String, String, Option<String>, DateTime<Utc>, String, String, String, bool, bool)>
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if logs.is_empty() {
            return Ok(());
        }
        
        // DB 연결 가져오기
        let executor = QueryExecutor::get_instance().await?;
        let mut client = executor.get_client().await?; // client is now deadpool_postgres::Client
        
        // 트랜잭션 시작
        let tx = client.transaction().await?;
        
        // 배치 크기에 따라 다른 방식으로 저장
        if logs.len() >= LOG_BATCH_SIZE {
            // 대량 배치 - 복사 모드 사용
            self.save_request_logs_batch(&tx, &logs).await?;
        } else {
            // 소량 배치 - 개별 삽입
            self.save_request_logs_individually(&tx, &logs).await?;
        }
        
        // 트랜잭션 커밋
        tx.commit().await?;
        
        Ok(())
    }
    
    /// 요청 로그 개별 저장
    async fn save_request_logs_individually(
        &self,
        tx: &Transaction<'_>, // Changed from client: &Client
        logs: &[(String, String, String, String, Option<String>, DateTime<Utc>, String, String, String, bool, bool)]
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        for (host, method, path, header, body, timestamp, session_id, client_ip, target_ip, is_rejected, is_tls) in logs {
            // 개별 로그 저장
            tx.execute( // Changed from client.execute
                request_logs::INSERT_LOG,
                &[
                    &host, &method, &path, &header, &body, &timestamp, 
                    &session_id, &client_ip, &target_ip, &is_rejected, &is_tls
                ]
            ).await?;
        }
        
        Ok(())
    }
    
    /// 요청 로그 배치 저장
    async fn save_request_logs_batch(
        &self,
        tx: &Transaction<'_>,
        logs: &[(String, String, String, String, Option<String>, DateTime<Utc>, String, String, String, bool, bool)]
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let copy_stmt = tx.prepare(request_logs::COPY_LOGS).await?;
        let sink = tx.copy_in(&copy_stmt).await?;
        
        let types = &[
            Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT,
            Type::TIMESTAMPTZ, Type::TEXT, Type::TEXT, Type::TEXT,
            Type::BOOL, Type::BOOL
        ];
        let writer = tokio_postgres::binary_copy::BinaryCopyInWriter::new(sink, types);
        let mut writer = std::pin::pin!(writer);
        
        for (host, method, path, header, body, timestamp, session_id, client_ip, target_ip, is_rejected, is_tls) in logs {
            writer.as_mut().write(&[
                host as &(dyn ToSql + Sync), 
                method as &(dyn ToSql + Sync), 
                path as &(dyn ToSql + Sync), 
                header as &(dyn ToSql + Sync), 
                &body.as_deref() as &(dyn ToSql + Sync), 
                timestamp as &(dyn ToSql + Sync), 
                session_id as &(dyn ToSql + Sync), 
                client_ip as &(dyn ToSql + Sync), 
                target_ip as &(dyn ToSql + Sync), 
                is_rejected as &(dyn ToSql + Sync), 
                is_tls as &(dyn ToSql + Sync)
            ]).await?;
        }
        
        writer.finish().await?;
        
        Ok(())
    }
    
    /// 응답 로그 저장
    async fn save_response_logs(
        &self,
        logs: Vec<(String, u16, u64, usize, DateTime<Utc>, String, Option<String>)> 
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if logs.is_empty() {
            return Ok(());
        }
        
        // DB 연결 가져오기
        let executor = QueryExecutor::get_instance().await?;
        let mut client = executor.get_client().await?; // client is now deadpool_postgres::Client
        
        // 트랜잭션 시작
        let tx = client.transaction().await?;
        
        // 배치 크기에 따라 다른 방식으로 저장
        if logs.len() >= LOG_BATCH_SIZE {
            // 대량 배치 - 복사 모드 사용
            self.save_response_logs_batch(&tx, &logs).await?;
        } else {
            // 소량 배치 - 개별 삽입
            self.save_response_logs_individually(&tx, &logs).await?;
        }
        
        // 트랜잭션 커밋
        tx.commit().await?;
        
        Ok(())
    }
    
    /// 응답 로그 개별 저장
    async fn save_response_logs_individually(
        &self,
        tx: &Transaction<'_>, // Changed from client: &Client
        logs: &[(String, u16, u64, usize, DateTime<Utc>, String, Option<String>)]
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        for (session_id, status_code, response_time, response_size, timestamp, headers, body_preview) in logs {
            // 개별 로그 저장
            tx.execute( // Changed from client.execute
                response_logs::INSERT_LOG,
                &[
                    &session_id, &(*status_code as i32), &(*response_time as i64), &(*response_size as i64),
                    &timestamp, &headers, &body_preview
                ]
            ).await?;
        }
        
        Ok(())
    }
    
    /// 응답 로그 배치 저장
    async fn save_response_logs_batch(
        &self,
        tx: &Transaction<'_>, // Changed from client: &Client
        logs: &[(String, u16, u64, usize, DateTime<Utc>, String, Option<String>)]
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 복사 모드 시작
        let copy_stmt = tx.prepare(response_logs::COPY_LOGS).await?; // Changed from client.prepare
        let sink = tx.copy_in(&copy_stmt).await?; // Changed from client.copy_in
        
        let types = &[
            Type::TEXT,        // session_id
            Type::INT2,        // status_code (u16 -> i16 for SMALLINT)
            Type::INT8,        // response_time (u64 -> i64 for BIGINT)
            Type::INT8,        // response_size (usize -> i64 for BIGINT)
            Type::TIMESTAMPTZ, // timestamp
            Type::TEXT,        // headers
            Type::TEXT         // body_preview (Option<String>)
        ];
        let writer = tokio_postgres::binary_copy::BinaryCopyInWriter::new(sink, types);
        let mut writer = std::pin::pin!(writer);
        
        // 로그 데이터 쓰기
        for (session_id, status_code, response_time, response_size, timestamp, headers, body_preview) in logs {
            // 각 필드 쓰기
            let status_code_i16 = *status_code as i16;
            let response_time_i64 = *response_time as i64;
            let response_size_i64 = *response_size as i64;
            
            writer.as_mut().write(&[
                session_id as &(dyn ToSql + Sync),
                &status_code_i16 as &(dyn ToSql + Sync),
                &response_time_i64 as &(dyn ToSql + Sync),
                &response_size_i64 as &(dyn ToSql + Sync),
                timestamp as &(dyn ToSql + Sync),
                headers as &(dyn ToSql + Sync),
                &body_preview.as_deref() as &(dyn ToSql + Sync)
            ]).await?;
        }
        
        // 복사 모드 종료
        writer.finish().await?;
        
        Ok(())
    }
    
    /// 요청 로그 배치가 플러시 필요한지 확인
    pub fn should_flush_request_logs(&self) -> bool {
        // 비동기 컨텍스트 외부에서 호출되므로 blocking으로 확인
        let batch = self.request_batch.try_lock();
        
        match batch {
            Ok(guard) => guard.count() >= LOG_BATCH_SIZE,
            Err(_) => false,  // 락 획득 실패 시 플러시하지 않음
        }
    }
    
    /// 응답 로그 배치가 플러시 필요한지 확인
    pub fn should_flush_response_logs(&self) -> bool {
        // 비동기 컨텍스트 외부에서 호출되므로 blocking으로 확인
        let batch = self.response_batch.try_lock();
        
        match batch {
            Ok(guard) => guard.count() >= LOG_BATCH_SIZE,
            Err(_) => false,  // 락 획득 실패 시 플러시하지 않음
        }
    }
    
    /// 응답 시간 업데이트
    pub async fn update_response_time(&self, session_id: &str, response_time: u64) -> Result<(), Box<dyn Error + Send + Sync>> {
        // DB 연결 가져오기
        let executor = QueryExecutor::get_instance().await?;
        
        // 응답 시간 업데이트 쿼리 실행
        executor.execute_query(
            response_logs::UPDATE_RESPONSE_TIME,
            &[&session_id, &(response_time as i64)]
        ).await?;
        
        Ok(())
    }
}