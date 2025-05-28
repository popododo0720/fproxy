use std::error::Error;
use chrono::{Local, Datelike, Duration};
use log::{debug, error, info};
use tokio_postgres::Client;

use super::pool::get_client;
use super::config::DbConfig;

/// 파티션 관리자 구조체
pub struct PartitionManager {
}

impl PartitionManager {
    /// 새 PartitionManager 인스턴스 생성
    pub fn new(_config: DbConfig) -> Self {
        Self { }
    }
    
    /// 파티션 관리 상태 확인 및 필요한 파티션 생성
    pub async fn ensure_partitions(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let client = get_client().await?;
        
        // 테이블 생성
        self.create_tables(&client).await?;
        
        // 파티션 생성 - 오늘과 내일 파티션만 생성
        let today = Local::now().date_naive();
        
        // 요청 로그 파티션 생성
        info!("request_logs 테이블의 파티션 생성 (오늘 및 내일)");
        self.create_partitions(&client, "request_logs", today, 2).await?;
        
        // 오래된 파티션 삭제
        let older_than_days = 365; // 1년 이상 된 파티션 삭제
        
        let request_deleted = self.drop_old_partitions(&client, "request_logs", older_than_days).await?;
        info!("{} 개의 오래된 request_logs 파티션 삭제됨", request_deleted);
        
        Ok(())
    }
    
    /// 테이블 생성
    async fn create_tables(&self, client: &Client) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 요청 로그 테이블 생성 - 헤더와 바디를 분리
        let request_logs_query = "
            CREATE TABLE IF NOT EXISTS request_logs (
                id SERIAL,
                host TEXT NOT NULL,
                method TEXT NOT NULL,
                path TEXT NOT NULL,
                header TEXT NOT NULL,
                body TEXT,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                session_id TEXT NOT NULL,
                client_ip TEXT NOT NULL,
                target_ip TEXT NOT NULL,
                response_time BIGINT,
                is_rejected BOOLEAN NOT NULL DEFAULT FALSE,
                is_tls BOOLEAN NOT NULL DEFAULT FALSE
            ) PARTITION BY RANGE (timestamp)".to_string();
        
        client.execute(&request_logs_query, &[]).await?;
        
        // 인덱스 생성
        let indices = [
            "CREATE INDEX IF NOT EXISTS request_logs_host_idx ON request_logs(host)",
            "CREATE INDEX IF NOT EXISTS request_logs_timestamp_idx ON request_logs(timestamp)",
            "CREATE INDEX IF NOT EXISTS request_logs_is_rejected_idx ON request_logs(is_rejected)",
            "CREATE INDEX IF NOT EXISTS request_logs_is_tls_idx ON request_logs(is_tls)"
        ];
        
        for index_query in indices {
            client.execute(index_query, &[]).await?;
        }
        
        Ok(())
    }
    
    /// 다중 파티션 생성
    async fn create_partitions(
        &self,
        client: &Client,
        table_name: &str,
        start_date: chrono::NaiveDate,
        num_days: i32
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let mut created_partitions = Vec::new();
        
        for i in 0..num_days {
            let date = start_date + Duration::days(i as i64);
            let partition_name = self.get_partition_name(table_name, date);
            
            // 파티션이 이미 존재하는지 확인
            if !self.partition_exists(client, &partition_name).await? {
                // 타임아웃 설정으로 파티션 생성
                match tokio::time::timeout(
                    tokio::time::Duration::from_secs(5), // 5초 타임아웃
                    self.create_partition(client, table_name, date)
                ).await {
                    Ok(Ok(name)) => {
                        created_partitions.push(name);
                    },
                    Ok(Err(e)) => {
                        error!("파티션 생성 실패 - {}: {}", partition_name, e);
                    },
                    Err(_) => {
                        error!("파티션 생성 타임아웃 - {}", partition_name);
                    }
                }
            } else {
                debug!("파티션이 이미 존재함: {}", partition_name);
            }
        }
        
        info!("테이블 {} 에 대해 {} 개의 파티션 생성됨", table_name, created_partitions.len());
        Ok(created_partitions)
    }
    
    /// 단일 파티션 생성
    async fn create_partition(
        &self,
        client: &Client,
        table_name: &str, 
        date: chrono::NaiveDate
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
        let partition_name = self.get_partition_name(table_name, date);
        let next_date = date.succ_opt().unwrap_or(date);
        
        // 파티션 생성
        let create_partition_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} PARTITION OF {} 
             FOR VALUES FROM ('{}') TO ('{}')",
            partition_name, table_name,
            date, next_date
        );
        
        client.execute(&create_partition_sql, &[]).await?;
        
        // 공통 인덱스 생성
        let host_idx_sql = format!(
            "CREATE INDEX IF NOT EXISTS {}_host_idx ON {} (host)",
            partition_name, partition_name
        );
        client.execute(&host_idx_sql, &[]).await?;
        
        let timestamp_idx_sql = format!(
            "CREATE INDEX IF NOT EXISTS {}_timestamp_idx ON {} (timestamp)",
            partition_name, partition_name
        );
        client.execute(&timestamp_idx_sql, &[]).await?;
        
        // 테이블별 특수 인덱스 생성
        if table_name == "reject_logs" {
            let client_ip_idx_sql = format!(
                "CREATE INDEX IF NOT EXISTS {}_client_ip_idx ON {} (client_ip)",
                partition_name, partition_name
            );
            client.execute(&client_ip_idx_sql, &[]).await?;
        } else if table_name == "request_logs" {
            let client_ip_idx_sql = format!(
                "CREATE INDEX IF NOT EXISTS {}_client_ip_idx ON {} (client_ip)",
                partition_name, partition_name
            );
            client.execute(&client_ip_idx_sql, &[]).await?;
            
            let target_ip_idx_sql = format!(
                "CREATE INDEX IF NOT EXISTS {}_target_ip_idx ON {} (target_ip)",
                partition_name, partition_name
            );
            client.execute(&target_ip_idx_sql, &[]).await?;
        }
        
        debug!("파티션 생성 완료: {}", partition_name);
        Ok(partition_name)
    }
    
    /// 파티션 이름 생성
    fn get_partition_name(&self, table_name: &str, date: chrono::NaiveDate) -> String {
        let year = date.year();
        let month = date.month();
        let day = date.day();
        
        format!("{}_{:04}{:02}{:02}", 
            table_name, 
            year, 
            month, 
            day
        )
    }
    
    /// 파티션이 존재하는지 확인
    async fn partition_exists(
        &self,
        client: &Client,
        partition_name: &str
    ) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let exists_query = "
            SELECT EXISTS (
                SELECT FROM pg_tables 
                WHERE schemaname = 'public' 
                AND tablename = $1
            )".to_string();
        
        let row = client.query_one(&exists_query, &[&partition_name]).await?;
        let exists: bool = row.get(0);
        
        Ok(exists)
    }
    
    /// 오래된 파티션 삭제
    async fn drop_old_partitions(
        &self,
        client: &Client,
        table_name: &str,
        older_than_days: i32
    ) -> Result<usize, Box<dyn Error + Send + Sync>> {
        let cutoff_date = Local::now().naive_local().date() - Duration::days(older_than_days as i64);
        
        // 테이블 목록 조회
        let tables_query = "
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public' AND tablename LIKE $1
        ".to_string();
        
        let pattern = format!("{}_%", table_name);
        let rows = client.query(&tables_query, &[&pattern]).await?;
        
        let mut dropped_count = 0;
        
        for row in rows {
            let partition_name: String = row.get(0);
            
            // 파티션 이름에서 날짜 추출 (table_YYYYMMDD 형식)
            if let Some(date_str) = partition_name.strip_prefix(&format!("{}_", table_name)) {
                if date_str.len() == 8 {
                    // YYYYMMDD 형식 파싱
                    if let Ok(year) = date_str[0..4].parse::<i32>() {
                        if let Ok(month) = date_str[4..6].parse::<u32>() {
                            if let Ok(day) = date_str[6..8].parse::<u32>() {
                                // 날짜 객체 생성
                                if let Some(partition_date) = chrono::NaiveDate::from_ymd_opt(year, month, day) {
                                    // 기준일보다 오래된 파티션 삭제
                                    if partition_date < cutoff_date {
                                        let drop_sql = format!("DROP TABLE IF EXISTS {}", partition_name);
                                        match client.execute(&drop_sql, &[]).await {
                                            Ok(_) => {
                                                info!("오래된 파티션 삭제: {}", partition_name);
                                                dropped_count += 1;
                                            },
                                            Err(e) => {
                                                error!("파티션 삭제 실패 - {}: {}", partition_name, e);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        info!("테이블 {} 에 대해 {} 개의 오래된 파티션 삭제됨", table_name, dropped_count);
        Ok(dropped_count)
    }
}

/// 파티션 상태 확인 함수 (전역 함수)
pub async fn ensure_partitions() -> Result<(), Box<dyn Error + Send + Sync>> {
    info!("데이터베이스 파티션 상태 확인 시작...");
    
    // DB 설정 로드
    match DbConfig::get() {
        Ok(db_config) => {
            debug!("DB 설정 로드 성공: {}:{}/{}", 
                   db_config.connection.host, 
                   db_config.connection.port, 
                   db_config.connection.database);
            
            // 파티션 관리자 생성 및 파티션 확인 - 타임아웃 적용
            let partition_manager = PartitionManager::new(db_config);
            match tokio::time::timeout(
                tokio::time::Duration::from_secs(60), // 60초 타임아웃
                partition_manager.ensure_partitions()
            ).await {
                Ok(Ok(_)) => {
                    info!("데이터베이스 파티션 상태 확인 완료");
                    Ok(())
                },
                Ok(Err(e)) => {
                    error!("데이터베이스 파티션 확인 실패: {}", e);
                    Err(e)
                },
                Err(_) => {
                    error!("데이터베이스 파티션 확인 타임아웃 발생, 계속 진행합니다");
                    Ok(()) // 타임아웃 발생 시에도 프로그램이 계속 실행되도록 Ok 반환
                }
            }
        },
        Err(e) => {
            error!("DB 설정 로드 실패: {}", e);
            Err(e)
        }
    }
} 