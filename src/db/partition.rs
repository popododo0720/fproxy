use std::error::Error;
use chrono::{Local, Datelike, Duration};
use log::{debug, error, info};
use tokio_postgres::Client;
use std::time::Duration as StdDuration;
use humantime;

use super::pool::get_client;
use super::config::DbConfig;
use crate::constants::request_logs;
use crate::constants::proxy_stats;
use crate::constants::proxy_stats_hourly;
use crate::constants::partition;

/// 테이블 유형 정의
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TableType {
    RequestLogs,
    ProxyStats,
    ProxyStatsHourly,
}

impl TableType {
    /// 테이블 이름 반환
    fn get_name(&self) -> &'static str {
        match self {
            TableType::RequestLogs => "request_logs",
            TableType::ProxyStats => "proxy_stats",
            TableType::ProxyStatsHourly => "proxy_stats_hourly",
        }
    }
}

/// 파티션 관리자 구조체
pub struct PartitionManager {
    config: DbConfig,
}

impl PartitionManager {
    /// 새 PartitionManager 인스턴스 생성
    pub fn new(config: DbConfig) -> Self {
        Self { config }
    }
    
    /// 파티션 관리 상태 확인 및 필요한 파티션 생성
    pub async fn ensure_partitions(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let client = get_client().await?;
        
        // 테이블 생성
        self.create_tables(&client).await?;
        
        // 파티션 생성 - 오늘과 미래 파티션 생성
        let today = Local::now().date_naive();
        let future_partitions = self.config.partitioning.future_partitions as i32;
        
        // 각 테이블별 파티션 생성
        info!("테이블별 파티션 생성 (오늘 및 미래 {} 일분)", future_partitions);
        
        // request_logs 테이블 파티션 생성
        info!("request_logs 테이블의 파티션 생성");
        self.create_partitions(&client, TableType::RequestLogs, today, future_partitions + 1).await?;
        
        // proxy_stats 테이블 파티션 생성
        info!("proxy_stats 테이블의 파티션 생성");
        self.create_partitions(&client, TableType::ProxyStats, today, future_partitions + 1).await?;
        
        // proxy_stats_hourly 테이블 파티션 생성
        info!("proxy_stats_hourly 테이블의 파티션 생성");
        self.create_partitions(&client, TableType::ProxyStatsHourly, today, future_partitions + 1).await?;
        
        // 오래된 파티션 삭제
        let older_than_days = self.config.partitioning.retention_period as i32;
        
        // 각 테이블별 오래된 파티션 삭제
        let request_deleted = self.drop_old_partitions(&client, TableType::RequestLogs, older_than_days).await?;
        info!("{} 개의 오래된 request_logs 파티션 삭제됨", request_deleted);
        
        let proxy_stats_deleted = self.drop_old_partitions(&client, TableType::ProxyStats, older_than_days).await?;
        info!("{} 개의 오래된 proxy_stats 파티션 삭제됨", proxy_stats_deleted);
        
        let proxy_stats_hourly_deleted = self.drop_old_partitions(&client, TableType::ProxyStatsHourly, older_than_days).await?;
        info!("{} 개의 오래된 proxy_stats_hourly 파티션 삭제됨", proxy_stats_hourly_deleted);
        
        Ok(())
    }
    
    /// 테이블 생성
    async fn create_tables(&self, client: &Client) -> Result<(), Box<dyn Error + Send + Sync>> {
        // request_logs 테이블 생성
        client.execute(request_logs::CREATE_TABLE, &[]).await?;
        
        // proxy_stats 테이블 생성
        client.execute(proxy_stats::CREATE_TABLE, &[]).await?;
        
        // proxy_stats_hourly 테이블 생성
        client.execute(proxy_stats_hourly::CREATE_TABLE, &[]).await?;
        
        // 인덱스 생성 - request_logs
        for index_query in request_logs::CREATE_INDICES {
            client.execute(index_query, &[]).await?;
        }
        
        // 인덱스 생성 - proxy_stats
        for index_query in proxy_stats::CREATE_INDICES {
            client.execute(index_query, &[]).await?;
        }
        
        // 인덱스 생성 - proxy_stats_hourly
        for index_query in proxy_stats_hourly::CREATE_INDICES {
            client.execute(index_query, &[]).await?;
        }
        
        Ok(())
    }
    
    /// 다중 파티션 생성
    async fn create_partitions(
        &self,
        client: &Client,
        table_type: TableType,
        start_date: chrono::NaiveDate,
        num_days: i32
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let mut created_partitions = Vec::new();
        let table_name = table_type.get_name();
        
        for i in 0..num_days {
            let date = start_date + Duration::days(i as i64);
            let partition_name = self.get_partition_name(table_name, date);
            
            // 파티션이 이미 존재하는지 확인
            if !self.partition_exists(client, &partition_name).await? {
                // 타임아웃 설정으로 파티션 생성
                match tokio::time::timeout(
                    tokio::time::Duration::from_secs(5), // 5초 타임아웃
                    self.create_partition(client, table_type, date)
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
        table_type: TableType, 
        date: chrono::NaiveDate
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
        let table_name = table_type.get_name();
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
        
        // 테이블별 인덱스 생성
        match table_type {
            TableType::RequestLogs => {
                // timestamp 인덱스 생성
                let timestamp_idx_sql = format!(
                    "CREATE INDEX IF NOT EXISTS {}_timestamp_idx ON {} (timestamp)",
                    partition_name, partition_name
                );
                client.execute(&timestamp_idx_sql, &[]).await?;
                
                // host 인덱스 생성
                let host_idx_sql = format!(
                    "CREATE INDEX IF NOT EXISTS {}_host_idx ON {} (host)",
                    partition_name, partition_name
                );
                client.execute(&host_idx_sql, &[]).await?;
                
                // 기타 request_logs 전용 인덱스 생성
                for index_sql in request_logs::create_partition_indices(&partition_name) {
                    client.execute(&index_sql, &[]).await?;
                }
            },
            TableType::ProxyStats => {
                // timestamp 인덱스 생성
                let timestamp_idx_sql = format!(
                    "CREATE INDEX IF NOT EXISTS {}_timestamp_idx ON {} (timestamp)",
                    partition_name, partition_name
                );
                client.execute(&timestamp_idx_sql, &[]).await?;
                
                // host 인덱스 생성
                let host_idx_sql = format!(
                    "CREATE INDEX IF NOT EXISTS {}_host_idx ON {} (host)",
                    partition_name, partition_name
                );
                client.execute(&host_idx_sql, &[]).await?;
                
                // 기타 proxy_stats 전용 인덱스 생성
                for index_sql in proxy_stats::create_partition_indices(&partition_name) {
                    client.execute(&index_sql, &[]).await?;
                }
            },
            TableType::ProxyStatsHourly => {
                // timestamp 인덱스 생성
                let timestamp_idx_sql = format!(
                    "CREATE INDEX IF NOT EXISTS {}_timestamp_idx ON {} (timestamp)",
                    partition_name, partition_name
                );
                client.execute(&timestamp_idx_sql, &[]).await?;
                
                // host 인덱스 생성
                let host_idx_sql = format!(
                    "CREATE INDEX IF NOT EXISTS {}_host_idx ON {} (host)",
                    partition_name, partition_name
                );
                client.execute(&host_idx_sql, &[]).await?;
                
                // 기타 proxy_stats_hourly 전용 인덱스 생성
                for index_sql in proxy_stats_hourly::create_partition_indices(&partition_name) {
                    client.execute(&index_sql, &[]).await?;
                }
            }
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
        let row = client.query_one(partition::CHECK_PARTITION_EXISTS, &[&partition_name]).await?;
        let exists: bool = row.get(0);
        
        Ok(exists)
    }
    
    /// 오래된 파티션 삭제
    async fn drop_old_partitions(
        &self,
        client: &Client,
        table_type: TableType,
        older_than_days: i32
    ) -> Result<usize, Box<dyn Error + Send + Sync>> {
        let table_name = table_type.get_name();
        let cutoff_date = Local::now().naive_local().date() - Duration::days(older_than_days as i64);
        
        // 테이블 목록 조회
        let pattern = format!("{}_%", table_name);
        let rows = client.query(partition::LIST_TABLES_QUERY, &[&pattern]).await?;
        
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
        
        Ok(dropped_count)
    }
    
    /// 파티션 자동 생성 백그라운드 작업 시작
    pub async fn start_partition_scheduler(config: DbConfig) {
        let future_partitions = config.partitioning.future_partitions as i32;
        
        tokio::spawn(async move {
            info!("파티션 자동 생성 스케줄러 시작");
            
            // 먼저 파티션 매니저 생성
            let partition_manager = PartitionManager::new(config.clone());
            
            loop {
                // 다음 날짜 계산 (자정 기준)
                let now = Local::now();
                let tomorrow = (now + Duration::days(1))
                    .date_naive()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                
                // 자정 5분 전 시간 계산
                let five_mins_before = tomorrow - Duration::minutes(5);
                
                // 지금부터 자정 5분 전까지의 시간 계산
                let time_until_check = if five_mins_before > now.naive_local() {
                    five_mins_before - now.naive_local()
                } else {
                    // 이미 자정을 지났다면 다음날 자정을 기준으로 다시 계산
                    let day_after_tomorrow = (now + Duration::minutes(2))
                        .date_naive()
                        .and_hms_opt(0, 0, 0)
                        .unwrap();
                    let five_mins_before_next = day_after_tomorrow - Duration::minutes(5);
                    five_mins_before_next - now.naive_local()
                };
                
                let sleep_duration = time_until_check.num_milliseconds() as u64;
                
                debug!("다음 파티션 자동 생성 스케줄: {} 후 ({})", 
                       humantime::format_duration(StdDuration::from_millis(sleep_duration)),
                       five_mins_before);
                
                // 계산된 시간만큼 대기
                tokio::time::sleep(StdDuration::from_millis(sleep_duration)).await;
                
                // 다음 날짜 파티션 생성
                let next_date = Local::now().date_naive() + Duration::days(1);
                info!("일일 파티션 생성 작업 시작: {}", next_date);
                
                match get_client().await {
                    Ok(client) => {
                        // request_logs 파티션 생성
                        match partition_manager.create_partitions(
                            &client, 
                            TableType::RequestLogs, 
                            next_date, 
                            future_partitions
                        ).await {
                            Ok(partitions) => {
                                if !partitions.is_empty() {
                                    info!("request_logs 미래 파티션 생성 완료: {} 개", partitions.len());
                                }
                            },
                            Err(e) => {
                                error!("request_logs 미래 파티션 생성 실패: {}", e);
                            }
                        }
                        
                        // proxy_stats 파티션 생성
                        match partition_manager.create_partitions(
                            &client, 
                            TableType::ProxyStats, 
                            next_date, 
                            future_partitions
                        ).await {
                            Ok(partitions) => {
                                if !partitions.is_empty() {
                                    info!("proxy_stats 미래 파티션 생성 완료: {} 개", partitions.len());
                                }
                            },
                            Err(e) => {
                                error!("proxy_stats 미래 파티션 생성 실패: {}", e);
                            }
                        }
                        
                        // proxy_stats_hourly 파티션 생성
                        match partition_manager.create_partitions(
                            &client, 
                            TableType::ProxyStatsHourly, 
                            next_date, 
                            future_partitions
                        ).await {
                            Ok(partitions) => {
                                if !partitions.is_empty() {
                                    info!("proxy_stats_hourly 미래 파티션 생성 완료: {} 개", partitions.len());
                                }
                            },
                            Err(e) => {
                                error!("proxy_stats_hourly 미래 파티션 생성 실패: {}", e);
                            }
                        }
                    },
                    Err(e) => {
                        error!("파티션 생성을 위한 DB 연결 실패: {}", e);
                    }
                }
                
                // 이후 자정까지 짧은 간격으로 대기 (자정을 놓치지 않도록)
                tokio::time::sleep(StdDuration::from_secs(10)).await;
            }
        });
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
            let partition_manager = PartitionManager::new(db_config.clone());
            match tokio::time::timeout(
                tokio::time::Duration::from_secs(60), // 60초 타임아웃
                partition_manager.ensure_partitions()
            ).await {
                Ok(Ok(_)) => {
                    info!("데이터베이스 파티션 상태 확인 완료");
                    
                    // 파티션 자동 생성 스케줄러 시작
                    PartitionManager::start_partition_scheduler(db_config).await;
                    
                    Ok(())
                },
                Ok(Err(e)) => {
                    error!("데이터베이스 파티션 확인 실패: {}", e);
                    Err(e)
                },
                Err(_) => {
                    error!("데이터베이스 파티션 확인 타임아웃 발생, 계속 진행합니다");
                    
                    // 타임아웃 발생해도 파티션 스케줄러는 시작
                    PartitionManager::start_partition_scheduler(db_config).await;
                    
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