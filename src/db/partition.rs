use std::error::Error;
use chrono::{Local, Datelike, Duration, Timelike};
use log::{debug, error, info};
use tokio_postgres::Client;

use super::pool::get_client;
use super::config::DbConfig;
use crate::constants::request_logs;
use crate::constants::proxy_stats;
use crate::constants::proxy_stats_hourly;

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
        
        // 익명 PL/pgSQL 블록 대신 각 파티션을 직접 생성
        for i in 0..num_days {
            let date = start_date + Duration::days(i as i64);
            let next_date = date + Duration::days(1);
            let partition_name = self.get_partition_name(table_name, date);
            
            // 파티션 생성 SQL
            let create_partition_sql = format!(
                "CREATE TABLE IF NOT EXISTS {} PARTITION OF {} 
                 FOR VALUES FROM ('{}') TO ('{}')",
                partition_name, table_name, date, next_date
            );
            
            // 파티션 생성 실행
            match client.execute(&create_partition_sql, &[]).await {
                Ok(_) => {
                    debug!("파티션 생성 완료: {}", partition_name);
                    created_partitions.push(partition_name.clone());
                    
                    // 파티션별 인덱스 생성
                    if let Err(e) = self.create_partition_indices(client, table_type, &partition_name).await {
                        error!("파티션 인덱스 생성 실패: {} - {}", partition_name, e);
                    }
                },
                Err(e) => {
                    // 이미 존재하는 파티션인 경우 무시하고 계속 진행
                    if e.to_string().contains("already exists") {
                        debug!("파티션이 이미 존재함: {}", partition_name);
                    } else {
                        error!("파티션 생성 실패: {} - {}", partition_name, e);
                    }
                }
            }
        }
        
        info!("파티션 자동 생성 완료: {}, {} 일 생성", table_name, created_partitions.len());
        Ok(created_partitions)
    }
    
    /// 파티션별 인덱스 생성
    async fn create_partition_indices(
        &self,
        client: &Client,
        table_type: TableType,
        partition_name: &str
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 테이블 유형에 따라 인덱스 생성
        match table_type {
            TableType::RequestLogs => {
                for index_sql in request_logs::create_partition_indices(partition_name) {
                    if let Err(e) = client.execute(&index_sql, &[]).await {
                        error!("인덱스 생성 실패: {} - {}", index_sql, e);
                    }
                }
            },
            TableType::ProxyStats => {
                for index_sql in proxy_stats::create_partition_indices(partition_name) {
                    if let Err(e) = client.execute(&index_sql, &[]).await {
                        error!("인덱스 생성 실패: {} - {}", index_sql, e);
                    }
                }
            },
            TableType::ProxyStatsHourly => {
                for index_sql in proxy_stats_hourly::create_partition_indices(partition_name) {
                    if let Err(e) = client.execute(&index_sql, &[]).await {
                        error!("인덱스 생성 실패: {} - {}", index_sql, e);
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// 단일 파티션 생성
    #[allow(dead_code)]
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
                // request_logs 테이블 전용 인덱스만 생성
                for index_sql in request_logs::create_partition_indices(&partition_name) {
                    client.execute(&index_sql, &[]).await?;
                }
            },
            TableType::ProxyStats => {
                // proxy_stats 테이블 전용 인덱스만 생성
                for index_sql in proxy_stats::create_partition_indices(&partition_name) {
                    client.execute(&index_sql, &[]).await?;
                }
            },
            TableType::ProxyStatsHourly => {
                // proxy_stats_hourly 테이블 전용 인덱스만 생성
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
    #[allow(dead_code)]
    async fn partition_exists(
        &self,
        client: &Client,
        partition_name: &str
    ) -> Result<bool, Box<dyn Error + Send + Sync>> {
        // 테이블 유형 확인
        let table_type = if partition_name.starts_with("proxy_stats_hourly") {
            TableType::ProxyStatsHourly
        } else if partition_name.starts_with("proxy_stats") {
            TableType::ProxyStats
        } else {
            TableType::RequestLogs
        };
        
        // 테이블 유형에 따라 적절한 쿼리 선택
        let check_query = match table_type {
            TableType::ProxyStatsHourly => crate::constants::proxy_stats_hourly::CHECK_PARTITION_EXISTS,
            TableType::ProxyStats => crate::constants::proxy_stats::CHECK_PARTITION_EXISTS,
            _ => "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = $1)"
        };
        
        match client.query_one(check_query, &[&partition_name]).await {
            Ok(row) => Ok(row.get::<_, bool>(0)),
            Err(e) => {
                error!("파티션 존재 여부 확인 실패: {}", e);
                Err(e.into())
            }
        }
    }
    
    /// 오래된 파티션 삭제
    async fn drop_old_partitions(
        &self,
        client: &Client,
        table_type: TableType,
        older_than_days: i32
    ) -> Result<usize, Box<dyn Error + Send + Sync>> {
        // 테이블 이름 가져오기
        let table_name = table_type.get_name();
        
        // 오래된 파티션 찾기
        let cutoff_date = chrono::Local::now().date_naive() - Duration::days(older_than_days as i64);
        let cutoff_str = format!("{:04}{:02}{:02}", cutoff_date.year(), cutoff_date.month(), cutoff_date.day());
        
        // 삭제할 파티션 조회
        let query = "
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public' 
            AND tablename LIKE $1 
            AND tablename < $2
        ";
        
        let like_pattern = format!("{}_%", table_name);
        let cutoff_pattern = format!("{}_{}", table_name, cutoff_str);
        
        let rows = match client.query(query, &[&like_pattern, &cutoff_pattern]).await {
            Ok(rows) => rows,
            Err(e) => {
                error!("오래된 파티션 조회 실패: {}", e);
                return Err(e.into());
            }
        };
        
        let mut dropped_count = 0;
        
        // 각 파티션 삭제
        for row in rows {
            let partition_name: String = row.get(0);
            let drop_query = format!("DROP TABLE IF EXISTS {}", partition_name);
            
            match client.execute(&drop_query, &[]).await {
                Ok(_) => {
                    debug!("오래된 파티션 삭제 완료: {}", partition_name);
                    dropped_count += 1;
                },
                Err(e) => {
                    error!("파티션 삭제 실패: {} - {}", partition_name, e);
                }
            }
        }
        
        info!("오래된 파티션 삭제 완료: {}, {} 개 삭제됨", table_name, dropped_count);
        Ok(dropped_count)
    }
    
    /// 파티션 자동 생성 백그라운드 작업 시작
    pub async fn start_partition_scheduler(config: DbConfig) {
        tokio::spawn(async move {
            info!("파티션 자동 생성 스케줄러 시작");
            
            // 파티션 매니저 생성
            let partition_manager = PartitionManager::new(config.clone());
            let creation_interval = config.partitioning.creation_interval as i32;
            let future_partitions = config.partitioning.future_partitions as i32;
            
            info!(
                "파티션 설정: 생성 주기 {} 일, 미래 파티션 {} 개, 보관 기간 {} 일", 
                creation_interval, 
                future_partitions, 
                config.partitioning.retention_period
            );
            
            // 파티션 생성 작업 실행 함수
            async fn execute_partition_tasks(
                partition_manager: &PartitionManager, 
                creation_interval: i32,
                future_partitions: i32
            ) {
                let today = Local::now().date_naive();
                info!("파티션 생성 작업 실행: 오늘 날짜 {}", today);
                
                // 현재 날짜부터 creation_interval 간격으로 미래 파티션 생성
                for i in 0..=future_partitions {
                    let target_date = today + Duration::days((i * creation_interval) as i64);
                    info!("파티션 생성 대상 날짜: {}", target_date);
                    create_all_partitions(partition_manager, target_date).await;
                }
                
                // 오늘 날짜에서 creation_interval 간격으로 다음 체크해야 할 날짜들 확인
                let mut next_check_dates = Vec::new();
                if creation_interval > 1 {
                    // 생성 간격이 1일 초과일 경우 다음 체크 날짜들을 계산
                    for i in 1..creation_interval {
                        let next_date = today + Duration::days(i as i64);
                        next_check_dates.push(next_date);
                    }
                    
                    if !next_check_dates.is_empty() {
                        info!("다음 {} 일 동안 체크할 파티션 생성 날짜: {:?}", 
                               creation_interval - 1, next_check_dates);
                    }
                }
                
                // 오래된 파티션 정리
                if let Ok(client) = get_client().await {
                    let retention_days = partition_manager.config.partitioning.retention_period as i32;
                    let tables = [
                        (TableType::RequestLogs, "request_logs"),
                        (TableType::ProxyStats, "proxy_stats"),
                        (TableType::ProxyStatsHourly, "proxy_stats_hourly")
                    ];
                    
                    for (table_type, name) in tables {
                        match partition_manager.drop_old_partitions(&client, table_type, retention_days).await {
                            Ok(count) => {
                                if count > 0 {
                                    info!("{} 개의 오래된 {} 파티션 삭제됨", count, name);
                                } else {
                                    debug!("삭제할 오래된 {} 파티션이 없음", name);
                                }
                            },
                            Err(e) => error!("오래된 {} 파티션 삭제 실패: {}", name, e)
                        }
                    }
                }
                
                info!("파티션 생성 작업 완료");
            }
            
            // 초기 파티션 생성
            execute_partition_tasks(&partition_manager, creation_interval, future_partitions).await;
            
            // 매일 자정 직전 파티션 생성을 위한 스케줄러 루프
            loop {
                // 현재 시간
                let now = Local::now();
                
                // 파티션 생성 주기가 1일 초과일 경우 오늘이 파티션 생성일인지 확인
                let is_creation_day = if creation_interval > 1 {
                    let days_since_epoch: i64 = now.date_naive().num_days_from_ce().into();
                    days_since_epoch % (creation_interval as i64) == 0
                } else {
                    true // 생성 주기가 1일이면 매일 실행
                };
                
                // 다음 자정까지 남은 시간 계산 (초 단위)
                let seconds_until_midnight = {
                    // 내일 자정의 NaiveDateTime 생성
                    let tomorrow_naive = (now.date_naive() + Duration::days(1))
                        .and_hms_opt(0, 0, 0)
                        .unwrap_or_else(|| now.date_naive().and_hms_opt(0, 0, 0).unwrap());
                    
                    // 현재 시간을 NaiveDateTime으로 변환
                    let now_naive = now.naive_local();
                    
                    // NaiveDateTime 간의 duration 계산
                    let duration = tomorrow_naive.signed_duration_since(now_naive);
                    duration.num_seconds() as u64
                };
                
                // 자정까지 남은 시간에 따라 다른 대기 시간 설정
                let wait_seconds = if seconds_until_midnight <= 3600 {
                    // 자정 1시간 전 - 파티션 생성 시작 (파티션 생성일이거나 creation_interval이 1일 경우)
                    if is_creation_day {
                        info!("자정이 {} 초 남음, 파티션 생성 준비", seconds_until_midnight);
                        if seconds_until_midnight <= 60 {
                            // 1분 이내로 남으면 바로 실행
                            0
                        } else {
                            // 자정 1시간 전에 맞추기 위해 대기
                            seconds_until_midnight - 3600
                        }
                    } else {
                        info!("오늘은 파티션 생성일이 아님 (주기: {} 일), 자정까지 대기", creation_interval);
                        seconds_until_midnight + 60 // 자정 이후 1분 더 대기
                    }
                } else {
                    info!("자정까지 {} 초 남음, 1시간 후 다시 확인", seconds_until_midnight);
                    3600 // 1시간
                };
                
                // 대기 시간이 있으면 대기
                if wait_seconds > 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(wait_seconds)).await;
                }
                
                // 자정 1시간 전인지 확인
                let now_after_wait = Local::now();
                let seconds_to_midnight = {
                    // 자정의 NaiveDateTime 생성
                    let midnight = now_after_wait.date_naive()
                        .and_hms_opt(0, 0, 0)
                        .unwrap();
                    
                    // 다음 자정 계산
                    let next_midnight = if now_after_wait.hour() < 12 {
                        // 현재 0시 이후면 다음 자정은 다음 날
                        midnight + Duration::days(1)
                    } else {
                        // 아니면 오늘 자정
                        midnight
                    };
                    
                    // 현재 시간을 NaiveDateTime으로 변환
                    let now_naive = now_after_wait.naive_local();
                    
                    // 자정까지 남은 시간 계산
                    let duration = next_midnight.signed_duration_since(now_naive);
                    duration.num_seconds() as i64
                };
                
                // 파티션 생성일인지 다시 확인 (대기 중 날짜가 바뀔 수 있음)
                let is_creation_day_after_wait = if creation_interval > 1 {
                    let days_since_epoch: i64 = now_after_wait.date_naive().num_days_from_ce().into();
                    days_since_epoch % (creation_interval as i64) == 0
                } else {
                    true // 생성 주기가 1일이면 매일 실행
                };
                
                // 자정 1시간 전(3600초) ~ 자정 사이이고 파티션 생성일인지 확인
                if seconds_to_midnight <= 3600 && seconds_to_midnight > 0 && is_creation_day_after_wait {
                    info!("자정 {}초 전, 파티션 생성 작업 시작 (creation_interval: {} 일)", 
                         seconds_to_midnight, creation_interval);
                    execute_partition_tasks(&partition_manager, creation_interval, future_partitions).await;
                    
                    // 파티션 생성 완료 후 자정이 지날 때까지 대기
                    let sleep_time = seconds_to_midnight as u64 + 60; // 자정 이후 1분 추가
                    tokio::time::sleep(std::time::Duration::from_secs(sleep_time)).await;
                }
                
                // 오늘 날짜의 마지막 파티션 생성 주기 확인
                let today = now_after_wait.date_naive();
                if creation_interval > 1 {
                    // 생성 주기가 1일 이상일 때 확인
                    let days_since_epoch: i64 = today.num_days_from_ce().into();
                    let creation_interval_i64 = creation_interval as i64;
                    let days_to_next_creation = (creation_interval_i64 - (days_since_epoch % creation_interval_i64)) % creation_interval_i64;
                    
                    if days_to_next_creation == 0 {
                        info!("오늘은 파티션 생성 주기({} 일)에 해당하는 날입니다", creation_interval);
                    } else {
                        debug!("다음 파티션 생성일까지 {} 일 남음 (주기: {} 일)", days_to_next_creation, creation_interval);
                    }
                }
            }
        });
    }

    /// 월별 파티션 생성 (proxy_stats_hourly 테이블용)
    async fn create_monthly_partitions(
        &self,
        client: &Client,
        table_type: TableType,
        start_date: chrono::NaiveDate,
        num_months: i32
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let mut created_partitions = Vec::new();
        let table_name = table_type.get_name();
        
        // 각 월별 파티션을 직접 생성
        for i in 0..num_months {
            // 월 계산
            let current_month = start_date.month() as i32;
            let current_year = start_date.year();
            
            let months_to_add = i;
            let year_offset = (current_month + months_to_add - 1) / 12;
            let new_month = ((current_month + months_to_add - 1) % 12) + 1;
            let new_year = current_year + year_offset;
            
            // 해당 월의 첫째 날과 다음 달의 첫째 날
            let month_start = chrono::NaiveDate::from_ymd_opt(new_year, new_month as u32, 1).unwrap();
            let next_month = if new_month == 12 {
                chrono::NaiveDate::from_ymd_opt(new_year + 1, 1, 1).unwrap()
            } else {
                chrono::NaiveDate::from_ymd_opt(new_year, (new_month + 1) as u32, 1).unwrap()
            };
            
            // 파티션 이름 (YYYYMM 형식)
            let partition_name = format!("{}_{:04}{:02}", table_name, new_year, new_month);
            
            // 파티션 생성 SQL
            let create_partition_sql = format!(
                "CREATE TABLE IF NOT EXISTS {} PARTITION OF {} 
                 FOR VALUES FROM ('{}') TO ('{}')",
                partition_name, table_name, month_start, next_month
            );
            
            // 파티션 생성 실행
            match client.execute(&create_partition_sql, &[]).await {
                Ok(_) => {
                    debug!("월간 파티션 생성 완료: {}", partition_name);
                    created_partitions.push(partition_name.clone());
                    
                    // 파티션별 인덱스 생성
                    if let Err(e) = self.create_partition_indices(client, table_type, &partition_name).await {
                        error!("월간 파티션 인덱스 생성 실패: {} - {}", partition_name, e);
                    }
                },
                Err(e) => {
                    // 이미 존재하는 파티션인 경우 무시하고 계속 진행
                    if e.to_string().contains("already exists") {
                        debug!("월간 파티션이 이미 존재함: {}", partition_name);
                    } else {
                        error!("월간 파티션 생성 실패: {} - {}", partition_name, e);
                    }
                }
            }
        }
        
        info!("월간 파티션 자동 생성 완료: {}, {} 개월 생성", table_name, created_partitions.len());
        Ok(created_partitions)
    }
}

// 모든 파티션 생성 헬퍼 함수
async fn create_all_partitions(partition_manager: &PartitionManager, target_date: chrono::NaiveDate) {
    // DB 연결 시도
    match get_client().await {
        Ok(client) => {
            // 테이블 타입별 파티션 생성
            let tables = [
                (TableType::RequestLogs, "request_logs"),
                (TableType::ProxyStats, "proxy_stats")
            ];
            
            for (table_type, name) in tables {
                match partition_manager.create_partitions(
                    &client, 
                    table_type, 
                    target_date, 
                    1  // 한 번에 하루치 파티션 생성
                ).await {
                    Ok(partitions) => {
                        if !partitions.is_empty() {
                            info!("{} 파티션 생성 완료: {} {}", name, target_date, partitions.len());
                        } else {
                            debug!("{} 파티션이 이미 모두 존재함: {}", name, target_date);
                        }
                    },
                    Err(e) => {
                        error!("{} 파티션 생성 실패: {} - {}", name, target_date, e);
                    }
                }
            }
            
            // 다음 달의 첫 날이면 월간 파티션도 생성
            let is_month_end = {
                let next_day = target_date + Duration::days(1);
                next_day.month() != target_date.month()
            };
            
            if is_month_end {
                // 다음 달의 첫 날 계산
                let next_month_date = {
                    let next_month = target_date.month() % 12 + 1;
                    let next_year = if next_month == 1 {
                        target_date.year() + 1
                    } else {
                        target_date.year()
                    };
                    chrono::NaiveDate::from_ymd_opt(next_year, next_month, 1).unwrap()
                };
                
                info!("월말 감지: 다음 달 ({}) 파티션 생성", next_month_date);
                
                // proxy_stats_hourly 월간 파티션 생성
                match partition_manager.create_monthly_partitions(
                    &client, 
                    TableType::ProxyStatsHourly, 
                    next_month_date, 
                    2 // 현재 월 + 다음 월 파티션 생성
                ).await {
                    Ok(partitions) => {
                        if !partitions.is_empty() {
                            info!("proxy_stats_hourly 월간 파티션 생성 완료: {} 개", partitions.len());
                        } else {
                            debug!("proxy_stats_hourly 월간 파티션이 이미 모두 존재함");
                        }
                    },
                    Err(e) => {
                        error!("proxy_stats_hourly 월간 파티션 생성 실패: {}", e);
                    }
                }
            }
        },
        Err(e) => {
            error!("파티션 생성을 위한 DB 연결 실패: {}", e);
        }
    }
}

/// 테이블 생성
async fn create_tables(client: &Client) -> Result<(), Box<dyn Error + Send + Sync>> {
    // 각 테이블을 개별적으로 생성하고 인덱스를 적용
    
    // 1. request_logs 테이블 생성 및 인덱스 적용
    match client.execute(request_logs::CREATE_TABLE, &[]).await {
        Ok(_) => {
            info!("request_logs 테이블 생성 확인 완료");
            // request_logs 인덱스 생성
            for index_query in request_logs::CREATE_INDICES {
                if let Err(e) = client.execute(index_query, &[]).await {
                    error!("request_logs 인덱스 생성 실패: {}", e);
                    // 인덱스 생성 실패는 무시하고 계속 진행
                }
            }
        },
        Err(e) => {
            error!("request_logs 테이블 생성 실패: {}", e);
            // 테이블 생성 실패는 무시하고 계속 진행
        }
    }
    
    // 2. proxy_stats 테이블 생성 및 인덱스 적용
    match client.execute(proxy_stats::CREATE_TABLE, &[]).await {
        Ok(_) => {
            info!("proxy_stats 테이블 생성 확인 완료");
            // proxy_stats 인덱스 생성
            for index_query in proxy_stats::CREATE_INDICES {
                if let Err(e) = client.execute(index_query, &[]).await {
                    error!("proxy_stats 인덱스 생성 실패: {}", e);
                    // 인덱스 생성 실패는 무시하고 계속 진행
                }
            }
        },
        Err(e) => {
            error!("proxy_stats 테이블 생성 실패: {}", e);
            // 테이블 생성 실패는 무시하고 계속 진행
        }
    }
    
    // 3. proxy_stats_hourly 테이블 생성 및 인덱스 적용
    match client.execute(proxy_stats_hourly::CREATE_TABLE, &[]).await {
        Ok(_) => {
            info!("proxy_stats_hourly 테이블 생성 확인 완료");
            // proxy_stats_hourly 인덱스 생성
            for index_query in proxy_stats_hourly::CREATE_INDICES {
                if let Err(e) = client.execute(index_query, &[]).await {
                    error!("proxy_stats_hourly 인덱스 생성 실패: {}", e);
                    // 인덱스 생성 실패는 무시하고 계속 진행
                }
            }
        },
        Err(e) => {
            error!("proxy_stats_hourly 테이블 생성 실패: {}", e);
            // 테이블 생성 실패는 무시하고 계속 진행
        }
    }
    
    // 모든 테이블 생성 시도 후 성공으로 처리
    Ok(())
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
            
            // 테이블 생성 확인
            let client = match get_client().await {
                Ok(client) => client,
                Err(e) => {
                    error!("DB 연결 실패: {}", e);
                    return Err(Box::new(e));
                }
            };
            
            // 테이블 생성 확인 - 오류가 발생해도 계속 진행
            if let Err(e) = create_tables(&client).await {
                error!("테이블 생성 중 일부 오류 발생: {}", e);
                // 오류가 발생해도 계속 진행
            } else {
                info!("테이블 생성 확인 완료");
            }
            
            // 오늘 날짜 계산
            let today = Local::now().date_naive();
            let future_partitions = db_config.partitioning.future_partitions as i32;
            
            // 각 테이블 유형별로 개별적으로 파티션 생성 시도
            // request_logs 테이블 파티션 생성 (일 단위)
            info!("request_logs 테이블의 파티션 생성");
            match partition_manager.create_partitions(&client, TableType::RequestLogs, today, future_partitions + 1).await {
                Ok(_) => info!("request_logs 파티션 생성 완료"),
                Err(e) => error!("request_logs 파티션 생성 실패: {}", e)
            }

            // proxy_stats 테이블 파티션 생성 (일 단위)
            info!("proxy_stats 테이블의 파티션 생성");
            match partition_manager.create_partitions(&client, TableType::ProxyStats, today, future_partitions + 1).await {
                Ok(_) => info!("proxy_stats 파티션 생성 완료"),
                Err(e) => error!("proxy_stats 파티션 생성 실패: {}", e)
            }
            
            // proxy_stats_hourly 테이블 파티션 생성 (월 단위)
            info!("proxy_stats_hourly 테이블의 월별 파티션 생성");
            // 현재 월의 첫날
            let current_month_start = chrono::NaiveDate::from_ymd_opt(today.year(), today.month(), 1).unwrap();
            
            match partition_manager.create_monthly_partitions(&client, TableType::ProxyStatsHourly, current_month_start, 2).await {
                Ok(_) => info!("proxy_stats_hourly 월별 파티션 생성 완료"),
                Err(e) => error!("proxy_stats_hourly 월별 파티션 생성 실패: {}", e)
            }
            
            // 오래된 파티션 삭제 시도
            let older_than_days = db_config.partitioning.retention_period as i32;
            
            // 각 테이블별 오래된 파티션 삭제
            match partition_manager.drop_old_partitions(&client, TableType::RequestLogs, older_than_days).await {
                Ok(count) => info!("{} 개의 오래된 request_logs 파티션 삭제됨", count),
                Err(e) => error!("request_logs 오래된 파티션 삭제 실패: {}", e)
            }
            
            match partition_manager.drop_old_partitions(&client, TableType::ProxyStats, older_than_days).await {
                Ok(count) => info!("{} 개의 오래된 proxy_stats 파티션 삭제됨", count),
                Err(e) => error!("proxy_stats 오래된 파티션 삭제 실패: {}", e)
            }
            
            match partition_manager.drop_old_partitions(&client, TableType::ProxyStatsHourly, older_than_days).await {
                Ok(count) => info!("{} 개의 오래된 proxy_stats_hourly 파티션 삭제됨", count),
                Err(e) => error!("proxy_stats_hourly 오래된 파티션 삭제 실패: {}", e)
            }
            
            info!("데이터베이스 파티션 상태 확인 완료");
            
            // 파티션 자동 생성 스케줄러 시작
            PartitionManager::start_partition_scheduler(db_config).await;
            
            Ok(())
        },
        Err(e) => {
            error!("DB 설정 로드 실패: {}", e);
            Err(e)
        }
    }
} 