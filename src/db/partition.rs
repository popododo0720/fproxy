use std::error::Error;
use log::{debug, error, info};
use tokio_postgres::Client;
use chrono::{Duration, Datelike, Timelike};

use crate::constants::{request_logs, proxy_stats, proxy_stats_hourly};
use crate::db::config::DbConfig;
use crate::db::pool::get_client;

/// 테이블 유형 열거형
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TableType {
    RequestLogs,
    ResponseLogs,
    ProxyStats,
    ProxyStatsHourly,
}

impl TableType {
    /// 테이블 이름 반환
    fn get_name(&self) -> &'static str {
        match self {
            TableType::RequestLogs => "request_logs",
            TableType::ResponseLogs => "response_logs",
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
    
    /// 파티션 생성
    async fn create_partitions(
        &self,
        client: &Client,
        table_type: TableType,
        start_date: chrono::NaiveDate,
        num_days: i32
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let mut created_partitions = Vec::new();
        let table_name = table_type.get_name();
        
        // 템플릿에 값을 직접 삽입하여 SQL 생성
        let sql = format!(
            include_str!("../constants/sql/create_future_partitions.sql"),
            table_name, num_days
        );
        
        // 직접 SQL 실행 대신 저장된 SQL 스크립트 사용
        match tokio::time::timeout(
            tokio::time::Duration::from_secs(10), // 10초 타임아웃
            client.execute(&sql, &[])
        ).await {
            Ok(Ok(_)) => {
                info!("파티션 자동 생성 스크립트 실행 완료: {}, {} 일 생성", table_name, num_days);
                
                // 생성된 파티션 이름 구성
                for i in 0..num_days {
                    let date = start_date + Duration::days(i as i64);
                    let partition_name = self.get_partition_name(table_name, date);
                    created_partitions.push(partition_name);
                }
                
                // 파티션별 인덱스 생성
                for partition_name in &created_partitions {
                    self.create_partition_indices(client, table_type, partition_name).await?;
                }
            },
            Ok(Err(e)) => {
                error!("파티션 생성 스크립트 실행 실패: {}", e);
                return Err(e.into());
            },
            Err(_) => {
                error!("파티션 생성 스크립트 실행 타임아웃");
                return Err("파티션 생성 스크립트 실행 타임아웃".into());
            }
        }
        
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
            TableType::ResponseLogs => {
                for index_sql in crate::constants::response_logs::create_partition_indices(partition_name) {
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
    
    /// 오래된 파티션 삭제
    async fn drop_old_partitions(
        &self,
        client: &Client,
        table_type: TableType,
        older_than_days: i32
    ) -> Result<usize, Box<dyn Error + Send + Sync>> {
        // 테이블 이름 가져오기
        let table_name = table_type.get_name();
        
        // 템플릿에 값을 직접 삽입하여 SQL 생성
        let sql = format!(
            include_str!("../constants/sql/drop_old_partitions.sql"),
            older_than_days, table_name
        );
        
        // 직접 SQL 실행 대신 저장된 SQL 스크립트 사용
        match tokio::time::timeout(
            tokio::time::Duration::from_secs(10), // 10초 타임아웃
            client.execute(&sql, &[])
        ).await {
            Ok(Ok(count)) => {
                info!("오래된 파티션 삭제 스크립트 실행 완료: {}, {} 일 이전 삭제", table_name, older_than_days);
                Ok(count as usize)
            },
            Ok(Err(e)) => {
                error!("오래된 파티션 삭제 스크립트 실행 실패: {}", e);
                Err(e.into())
            },
            Err(_) => {
                error!("오래된 파티션 삭제 스크립트 실행 타임아웃");
                Err("오래된 파티션 삭제 스크립트 실행 타임아웃".into())
            }
        }
    }
    
    /// 파티션 자동 생성 백그라운드 작업 시작
    pub async fn start_partition_scheduler(config: DbConfig) {
        tokio::spawn(async move {
            info!("파티션 자동 생성 스케줄러 시작");
            
            // 먼저 파티션 매니저 생성
            let partition_manager = PartitionManager::new(config.clone());
            
            // 파티션 생성 작업 실행 함수
            async fn execute_partition_tasks(partition_manager: &PartitionManager) {
                let today = chrono::Local::now().date_naive();
                info!("파티션 생성 작업 실행: 오늘 날짜 {}", today);
                
                // 오늘 파티션 생성
                create_all_partitions(partition_manager, today).await;
                
                // 내일 파티션 미리 생성
                let tomorrow = today + Duration::days(1);
                info!("내일 날짜 파티션 미리 생성: {}", tomorrow);
                create_all_partitions(partition_manager, tomorrow).await;
                
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
            execute_partition_tasks(&partition_manager).await;
            
            // 매일 자정 직후 파티션 생성을 위한 스케줄러 루프
            loop {
                // 현재 시간
                let now = chrono::Local::now();
                
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
                let wait_seconds = if seconds_until_midnight < 300 {
                    // 자정 5분 전 - 자정 직후로 타이밍 맞추기
                    info!("자정이 {} 초 남음, 자정 직후 파티션 생성 준비", seconds_until_midnight);
                    seconds_until_midnight + 5 // 자정 이후 5초 추가
                } else if seconds_until_midnight < 1800 { // 30분 미만
                    info!("자정까지 {} 초 남음, 10분 후 다시 확인", seconds_until_midnight);
                    600 // 10분
                } else if seconds_until_midnight < 7200 { // 2시간 미만
                    info!("자정까지 {} 초 남음, 30분 후 다시 확인", seconds_until_midnight);
                    1800 // 30분
                } else {
                    info!("자정까지 {} 초 남음, 1시간 후 다시 확인", seconds_until_midnight);
                    3600 // 1시간
                };
                
                // 지정된 시간만큼 대기
                tokio::time::sleep(std::time::Duration::from_secs(wait_seconds)).await;
                
                // 자정이 지났는지 확인 (0시 ~ 0시 5분 사이인지)
                let now_after_wait = chrono::Local::now();
                if now_after_wait.hour() == 0 && now_after_wait.minute() < 5 {
                    info!("자정 직후 감지됨, 파티션 생성 작업 시작");
                    execute_partition_tasks(&partition_manager).await;
                    
                    // 자정 직후 파티션 생성 완료했으면 충분히 대기 (1시간)
                    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
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
        
        // 템플릿에 값을 직접 삽입하여 SQL 생성
        let sql = format!(
            include_str!("../constants/sql/create_monthly_partitions.sql"),
            table_name, num_months
        );
        
        // 직접 SQL 실행 대신 저장된 SQL 스크립트 사용
        match tokio::time::timeout(
            tokio::time::Duration::from_secs(10), // 10초 타임아웃
            client.execute(&sql, &[])
        ).await {
            Ok(Ok(_)) => {
                info!("월간 파티션 자동 생성 스크립트 실행 완료: {}, {} 개월 생성", table_name, num_months);
                
                // 생성된 파티션 이름 구성
                for i in 0..num_months {
                    let current_month = start_date.month() as i32;
                    let current_year = start_date.year();
                    
                    let months_to_add = current_month + i - 1;
                    let year_offset = months_to_add / 12;
                    let new_month = (months_to_add % 12) + 1;
                    let new_year = current_year + year_offset;
                    
                    // 해당 월의 첫째 날
                    let date = chrono::NaiveDate::from_ymd_opt(new_year, new_month as u32, 1).unwrap();
                    let partition_name = format!("{}_{:04}{:02}", table_name, date.year(), date.month());
                    created_partitions.push(partition_name);
                }
                
                // 파티션별 인덱스 생성
                for partition_name in &created_partitions {
                    self.create_partition_indices(client, table_type, partition_name).await?;
                }
            },
            Ok(Err(e)) => {
                error!("월간 파티션 생성 스크립트 실행 실패: {}", e);
                return Err(e.into());
            },
            Err(_) => {
                error!("월간 파티션 생성 스크립트 실행 타임아웃");
                return Err("월간 파티션 생성 스크립트 실행 타임아웃".into());
            }
        }
        
        Ok(created_partitions)
    }
}

// 모든 파티션 생성 헬퍼 함수
async fn create_all_partitions(partition_manager: &PartitionManager, target_date: chrono::NaiveDate) {
    // DB 연결 시도
    match get_client().await {
        Ok(client) => {
            // future_partitions 값은 partition_manager의 config에서 가져온 값 사용
            let future_partitions = partition_manager.config.partitioning.future_partitions as i32;
            
            // 테이블 타입별 파티션 생성
            let tables = [
                (TableType::RequestLogs, "request_logs"),
                (TableType::ResponseLogs, "response_logs"),
                (TableType::ProxyStats, "proxy_stats")
            ];
            
            for (table_type, name) in tables {
                match partition_manager.create_partitions(
                    &client, 
                    table_type, 
                    target_date, 
                    future_partitions
                ).await {
                    Ok(partitions) => {
                        if !partitions.is_empty() {
                            info!("{} 파티션 생성 완료: {} 개", name, partitions.len());
                        } else {
                            debug!("{} 파티션이 이미 모두 존재함", name);
                        }
                    },
                    Err(e) => {
                        error!("{} 파티션 생성 실패: {}", name, e);
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
                    1 // 월간 파티션은 한 달만 생성
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
    
    // 2. response_logs 테이블 생성 및 인덱스 적용
    match client.execute(crate::constants::response_logs::CREATE_TABLE, &[]).await {
        Ok(_) => {
            info!("response_logs 테이블 생성 확인 완료");
            // response_logs 인덱스 생성
            for index_query in crate::constants::response_logs::CREATE_INDICES {
                if let Err(e) = client.execute(index_query, &[]).await {
                    error!("response_logs 인덱스 생성 실패: {}", e);
                    // 인덱스 생성 실패는 무시하고 계속 진행
                }
            }
        },
        Err(e) => {
            error!("response_logs 테이블 생성 실패: {}", e);
            // 테이블 생성 실패는 무시하고 계속 진행
        }
    }
    
    // 3. proxy_stats 테이블 생성 및 인덱스 적용
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
    
    // 4. proxy_stats_hourly 테이블 생성 및 인덱스 적용
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
            let today = chrono::Local::now().date_naive();
            let future_partitions = db_config.partitioning.future_partitions as i32;
            
            // 각 테이블 유형별로 개별적으로 파티션 생성 시도
            // request_logs 테이블 파티션 생성 (일 단위)
            info!("request_logs 테이블의 파티션 생성");
            match partition_manager.create_partitions(&client, TableType::RequestLogs, today, future_partitions + 1).await {
                Ok(_) => info!("request_logs 파티션 생성 완료"),
                Err(e) => error!("request_logs 파티션 생성 실패: {}", e)
            }

            // response_logs 테이블 파티션 생성 (일 단위)
            info!("response_logs 테이블의 파티션 생성");
            match partition_manager.create_partitions(&client, TableType::ResponseLogs, today, future_partitions + 1).await {
                Ok(_) => info!("response_logs 파티션 생성 완료"),
                Err(e) => error!("response_logs 파티션 생성 실패: {}", e)
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
            
            // response_logs 테이블 오래된 파티션 삭제
            match partition_manager.drop_old_partitions(&client, TableType::ResponseLogs, older_than_days).await {
                Ok(count) => info!("{} 개의 오래된 response_logs 파티션 삭제됨", count),
                Err(e) => error!("response_logs 오래된 파티션 삭제 실패: {}", e)
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
            Err(Box::new(e))
        }
    }
} 