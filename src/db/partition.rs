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
        
        // 기준 날짜 계산 (오늘 - older_than_days)
        let cutoff_date = Local::now().date_naive() - chrono::Duration::days(older_than_days as i64);
        let cutoff_str = format!("{:04}{:02}{:02}", cutoff_date.year(), cutoff_date.month(), cutoff_date.day());
        
        // 오래된 파티션 목록 조회
        let rows = client.query(
            crate::constants::partition::LIST_OLD_PARTITIONS, 
            &[&table_name, &cutoff_str]
        ).await?;
        
        let mut dropped_count = 0;
        
        // 각 파티션 삭제
        for row in rows {
            let partition_name: String = row.get(0);
            
            // 파티션 삭제 쿼리 실행
            let drop_query = crate::constants::partition::DROP_PARTITION.replace("$1", &partition_name);
            match client.execute(&drop_query, &[]).await {
                Ok(_) => {
                    debug!("오래된 파티션 삭제 완료: {}", partition_name);
                    dropped_count += 1;
                },
                Err(e) => {
                    error!("파티션 삭제 실패 - {}: {}", partition_name, e);
                }
            }
        }
        
        Ok(dropped_count)
    }
    
    /// 파티션 자동 생성 백그라운드 작업 시작
    pub async fn start_partition_scheduler(config: DbConfig) {
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
                        // future_partitions 값은 partition_manager의 config에서 가져온 값 사용
                        let future_partitions = partition_manager.config.partitioning.future_partitions as i32;
                        
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
                        // 다음 달의 첫 날 계산
                        let next_month_date = {
                            let next_month = next_date.month() % 12 + 1;
                            let next_year = if next_month == 1 {
                                next_date.year() + 1
                            } else {
                                next_date.year()
                            };
                            chrono::NaiveDate::from_ymd_opt(next_year, next_month, 1).unwrap()
                        };
                        
                        match partition_manager.create_monthly_partitions(&client, TableType::ProxyStatsHourly, next_month_date, future_partitions).await {
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

    /// proxy_stats_hourly 테이블을 위한 월 단위 파티션 생성
    async fn create_monthly_partitions(
        &self,
        client: &Client,
        table_type: TableType,
        start_month: chrono::NaiveDate,
        num_months: i32
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let mut created_partitions = Vec::new();
        let table_name = table_type.get_name();
        
        // 월별 파티션 생성
        for i in 0..num_months {
            // 시작 월에 i개월 추가
            let month_start = if i == 0 {
                start_month
            } else {
                // 월 추가 계산
                let year = start_month.year();
                let month = start_month.month() as i32 + i;
                let new_year = year + (month - 1) / 12;
                let new_month = ((month - 1) % 12) + 1;
                
                chrono::NaiveDate::from_ymd_opt(new_year, new_month as u32, 1).unwrap_or(start_month)
            };
            
            // 다음 월 계산
            let next_month = if month_start.month() == 12 {
                chrono::NaiveDate::from_ymd_opt(month_start.year() + 1, 1, 1).unwrap()
            } else {
                chrono::NaiveDate::from_ymd_opt(month_start.year(), month_start.month() + 1, 1).unwrap()
            };
            
            // 파티션 이름 생성 (월 단위)
            let partition_name = format!("{}_{:04}{:02}", 
                table_name, 
                month_start.year(), 
                month_start.month()
            );
            
            // 파티션이 이미 존재하는지 확인
            if !self.partition_exists(client, &partition_name).await? {
                // 타임아웃 설정으로 파티션 생성
                match tokio::time::timeout(
                    tokio::time::Duration::from_secs(5), // 5초 타임아웃
                    self.create_monthly_partition(client, table_type, month_start, next_month)
                ).await {
                    Ok(Ok(name)) => {
                        created_partitions.push(name);
                    },
                    Ok(Err(e)) => {
                        error!("월별 파티션 생성 실패 - {}: {}", partition_name, e);
                    },
                    Err(_) => {
                        error!("월별 파티션 생성 타임아웃 - {}", partition_name);
                    }
                }
            } else {
                debug!("월별 파티션이 이미 존재함: {}", partition_name);
            }
        }
        
        info!("테이블 {} 에 대해 {} 개의 월별 파티션 생성됨", table_name, created_partitions.len());
        Ok(created_partitions)
    }

    /// 단일 월별 파티션 생성
    async fn create_monthly_partition(
        &self,
        client: &Client,
        table_type: TableType, 
        month_start: chrono::NaiveDate,
        next_month: chrono::NaiveDate
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
        let table_name = table_type.get_name();
        // 월별 파티션 이름 생성
        let partition_name = format!("{}_{:04}{:02}", 
            table_name, 
            month_start.year(), 
            month_start.month()
        );
        
        // 파티션 생성
        let create_partition_sql = match table_type {
            TableType::ProxyStatsHourly => {
                // proxy_stats_hourly 테이블은 월 단위 파티션 포맷 사용
                let from_ts = format!("{}-{:02}-01 00:00:00", month_start.year(), month_start.month());
                let to_ts = format!("{}-{:02}-01 00:00:00", next_month.year(), next_month.month());
                
                debug!("월별 파티션 생성 쿼리 - 파티션: {}, 범위: {} ~ {}", partition_name, from_ts, to_ts);
                
                // 직접 쿼리 작성
                format!(
                    "{}",
                    crate::constants::partition::CREATE_MONTHLY_PARTITION
                ).replace("{}", &partition_name)
                 .replace("{}", &table_name)
                 .replace("{}", &from_ts)
                 .replace("{}", &to_ts)
            },
            _ => {
                // 다른 테이블은 기존 방식 사용
                format!(
                    "CREATE TABLE IF NOT EXISTS {} PARTITION OF {} 
                     FOR VALUES FROM ('{}') TO ('{}')",
                    partition_name, table_name,
                    month_start, next_month
                )
            }
        };
        
        client.execute(&create_partition_sql, &[]).await?;
        
        // proxy_stats_hourly 테이블 전용 인덱스 생성
        if table_type == TableType::ProxyStatsHourly {
            // 인덱스 생성 쿼리 포맷 사용
            let create_index_query = format!(
                "{}",
                crate::constants::proxy_stats_hourly::CREATE_PARTITION_INDEX_FORMAT
            ).replace("{}", &partition_name)
             .replace("{}", &partition_name);
             
            client.execute(&create_index_query, &[]).await?;
        }
        
        debug!("월별 파티션 생성 완료: {}", partition_name);
        Ok(partition_name)
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