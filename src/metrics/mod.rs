use std::sync::atomic::{ AtomicU64, Ordering };
use std::time::{ Duration, Instant };
use std::sync::{ Arc };

use tokio::time;
use log::{ info, debug, error, warn };
use once_cell::sync::Lazy;
use chrono::Utc;
use chrono::Datelike;

use crate::db;

// 전역 메트릭스 인스턴스를 위한 Lazy 정적 변수
static METRICS_INSTANCE: Lazy<Arc<Metrics>> = Lazy::new(|| {
    let metrics = Arc::new(Metrics::new_internal());
    
    // 주기적인 통계 로깅 설정 (비활성화)
    // 주기적인 통계 로깅 코드 제거
    
    // 주기적인 DB 저장 설정 (1초마다)
    let metrics_clone = Arc::clone(&metrics);
    tokio::spawn(async move {
        // DB 테이블 생성 확인
        if let Err(e) = Metrics::ensure_stats_table().await {
            error!("메트릭스 통계 테이블 생성 실패: {}", e);
        }
        
        let mut interval = time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            if let Err(e) = metrics_clone.save_stats_to_db().await {
                error!("메트릭스 통계 DB 저장 실패: {}", e);
            }
        }
    });
    
    metrics
});

pub struct Metrics {
    http_active_connections: AtomicU64,
    http_bytes_transferred_in: AtomicU64,
    http_bytes_transferred_out: AtomicU64,
    tls_active_connections: AtomicU64,
    tls_bytes_transferred_in: AtomicU64,
    tls_bytes_transferred_out: AtomicU64,
    start_time: Instant,
}

impl Metrics {
    // 싱글톤 인스턴스를 반환
    pub fn new() -> Arc<Self> {
        Arc::clone(&METRICS_INSTANCE)
    }
    
    // 내부 생성 함수
    fn new_internal() -> Self {
        Self {
            http_active_connections: AtomicU64::new(0),
            http_bytes_transferred_in: AtomicU64::new(0),
            http_bytes_transferred_out: AtomicU64::new(0),
            tls_active_connections: AtomicU64::new(0),
            tls_bytes_transferred_in: AtomicU64::new(0),
            tls_bytes_transferred_out: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    // 바이트를 MB로 변환
    fn bytes_to_mb(bytes: u64) -> f64 {
        let mb = bytes as f64 / (1024.0 * 1024.0);
        (mb * 1000.0).round() / 1000.0  // 소수점 세 자리로 제한
    }
    
    // proxy_stats 테이블 생성 확인
    async fn ensure_stats_table() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!("proxy_stats 테이블 확인 중...");
        
        // 쿼리 실행기 가져오기
        let executor = match db::query::QueryExecutor::get_instance().await {
            Ok(executor) => executor,
            Err(e) => {
                error!("쿼리 실행기 가져오기 실패: {}", e);
                return Err(e);
            }
        };
        
        // 테이블 존재 여부 확인
        let check_table_query = "
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'proxy_stats'
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
            debug!("proxy_stats 테이블이 존재하지 않습니다. 새로 생성합니다.");
            
            // 테이블 생성 쿼리 - 파티셔닝 추가
            let create_table_query = "
                CREATE TABLE IF NOT EXISTS proxy_stats (
                    id SERIAL,
                    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    http_active_connections BIGINT NOT NULL,
                    http_bytes_in DOUBLE PRECISION NOT NULL,
                    http_bytes_out DOUBLE PRECISION NOT NULL,
                    tls_active_connections BIGINT NOT NULL,
                    tls_bytes_in DOUBLE PRECISION NOT NULL,
                    tls_bytes_out DOUBLE PRECISION NOT NULL,
                    uptime_seconds BIGINT NOT NULL,
                    PRIMARY KEY (id, timestamp)
                ) PARTITION BY RANGE (timestamp)
            ";
            
            // 인덱스 생성 쿼리
            let create_index_query = "
                CREATE INDEX IF NOT EXISTS proxy_stats_timestamp_idx ON proxy_stats(timestamp)
            ";
            
            // 테이블 생성 실행
            if let Err(e) = executor.execute_query(create_table_query, &[]).await {
                error!("proxy_stats 테이블 생성 실패: {}", e);
                return Err(e);
            }
            
            // 인덱스 생성 실행
            if let Err(e) = executor.execute_query(create_index_query, &[]).await {
                error!("proxy_stats 인덱스 생성 실패: {}", e);
                // 인덱스 생성 실패는 치명적이지 않으므로 계속 진행
                warn!("proxy_stats 인덱스 생성 실패했지만 계속 진행합니다");
            }
            
            info!("proxy_stats 테이블이 성공적으로 생성되었습니다.");
        } else {
            debug!("proxy_stats 테이블이 이미 존재합니다.");
        }
        
        // 현재 날짜와 다음 날짜 파티션 생성
        let today = chrono::Local::now().date_naive();
        let tomorrow = today + chrono::Duration::days(1);
        let day_after_tomorrow = today + chrono::Duration::days(2);
        
        // 오늘 파티션 생성
        let today_partition = Self::create_stats_partition(&executor, today, tomorrow).await?;
        info!("오늘 파티션 생성 완료: {}", today_partition);
        
        // 내일 파티션 생성
        let tomorrow_partition = Self::create_stats_partition(&executor, tomorrow, day_after_tomorrow).await?;
        info!("내일 파티션 생성 완료: {}", tomorrow_partition);
        
        Ok(())
    }
    
    // 날짜별 파티션 생성 함수
    async fn create_stats_partition(
        executor: &db::query::QueryExecutor,
        start_date: chrono::NaiveDate,
        end_date: chrono::NaiveDate
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // 파티션 이름 생성
        let partition_name = format!("proxy_stats_{:04}{:02}{:02}", 
            start_date.year(), start_date.month(), start_date.day());
        
        // 파티션 존재 여부 확인
        let check_partition_query = "
            SELECT EXISTS (
                SELECT FROM pg_tables 
                WHERE schemaname = 'public' 
                AND tablename = $1
            )
        ";
        
        let partition_exists = match executor.query_one(check_partition_query, &[&partition_name], |row| Ok(row.get::<_, bool>(0))).await {
            Ok(exists) => exists,
            Err(e) => {
                error!("파티션 존재 여부 확인 실패: {}", e);
                false
            }
        };
        
        if !partition_exists {
            // 파티션 생성 쿼리
            let create_partition_query = format!(
                "CREATE TABLE IF NOT EXISTS {} PARTITION OF proxy_stats 
                 FOR VALUES FROM ('{}') TO ('{}')",
                partition_name, start_date, end_date
            );
            
            // 파티션 생성 실행
            if let Err(e) = executor.execute_query(&create_partition_query, &[]).await {
                error!("파티션 생성 실패: {}", e);
                return Err(e.into());
            }
            
            // 파티션별 인덱스 생성
            let create_index_query = format!(
                "CREATE INDEX IF NOT EXISTS {}_timestamp_idx ON {} (timestamp)",
                partition_name, partition_name
            );
            
            if let Err(e) = executor.execute_query(&create_index_query, &[]).await {
                warn!("파티션 인덱스 생성 실패: {}", e);
                // 인덱스 생성 실패는 치명적이지 않으므로 계속 진행
            }
            
            debug!("파티션 생성 완료: {}", partition_name);
        } else {
            debug!("파티션이 이미 존재함: {}", partition_name);
        }
        
        Ok(partition_name)
    }
    
    // 메트릭스 통계를 DB에 저장
    async fn save_stats_to_db(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 현재 메트릭스 데이터 수집
        let metrics = self.load_all_metrics();
        
        // 쿼리 실행기 가져오기
        let executor = match db::query::QueryExecutor::get_instance().await {
            Ok(executor) => executor,
            Err(e) => {
                error!("쿼리 실행기 가져오기 실패: {}", e);
                return Err(e);
            }
        };
        
        // 통계 데이터 삽입 쿼리
        let insert_query = "
            INSERT INTO proxy_stats (
                timestamp, 
                http_active_connections, 
                http_bytes_in, 
                http_bytes_out, 
                tls_active_connections, 
                tls_bytes_in, 
                tls_bytes_out, 
                uptime_seconds
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
        ";
        
        // 현재 시간
        let now = Utc::now();
        
        // 바이트를 메가바이트로 변환하고 소수점 세 자리로 제한
        let http_bytes_in_mb = (Self::bytes_to_mb(metrics.http_bytes_transferred_in) * 1000.0).round() / 1000.0;
        let http_bytes_out_mb = (Self::bytes_to_mb(metrics.http_bytes_transferred_out) * 1000.0).round() / 1000.0;
        let tls_bytes_in_mb = (Self::bytes_to_mb(metrics.tls_bytes_transferred_in) * 1000.0).round() / 1000.0;
        let tls_bytes_out_mb = (Self::bytes_to_mb(metrics.tls_bytes_transferred_out) * 1000.0).round() / 1000.0;
        
        // 쿼리 파라미터 (메가바이트 단위로 저장)
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
            &now,
            &(metrics.http_active_connections as i64),
            &http_bytes_in_mb,  // 메가바이트 단위
            &http_bytes_out_mb, // 메가바이트 단위
            &(metrics.tls_active_connections as i64),
            &tls_bytes_in_mb,   // 메가바이트 단위
            &tls_bytes_out_mb,  // 메가바이트 단위
            &(metrics.start_time as i64)
        ];
        
        // 쿼리 실행
        match executor.execute_query(insert_query, params).await {
            Ok(_) => {
                debug!("메트릭스 통계 DB 저장 완료");
                Ok(())
            },
            Err(e) => {
                // 파티션 관련 오류인 경우 파티션 생성 시도
                let error_msg = e.to_string();
                if error_msg.contains("no partition") || error_msg.contains("partition") {
                    error!("파티션 관련 오류 발생: {}, 파티션 생성 시도", error_msg);
                    
                    // 오늘 날짜와 내일 날짜 계산
                    let today = chrono::Local::now().date_naive();
                    let tomorrow = today + chrono::Duration::days(1);
                    
                    // 오늘 파티션 생성 시도
                    match Self::create_stats_partition(&executor, today, tomorrow).await {
                        Ok(partition_name) => {
                            info!("누락된 파티션 자동 생성: {}", partition_name);
                            
                            // 파티션 생성 후 다시 시도
                            match executor.execute_query(insert_query, params).await {
                                Ok(_) => {
                                    debug!("파티션 생성 후 메트릭스 통계 DB 저장 완료");
                                    return Ok(());
                                },
                                Err(e) => {
                                    error!("파티션 생성 후에도 메트릭스 통계 DB 저장 실패: {}", e);
                                    return Err(e.into());
                                }
                            }
                        },
                        Err(e) => {
                            error!("누락된 파티션 생성 실패: {}", e);
                            return Err(e);
                        }
                    }
                }
                
                error!("메트릭스 통계 DB 저장 실패: {}", e);
                Err(e.into())
            }
        }
    }

    // 모든 메트릭스 데이터를 구조체로 로드
    fn load_all_metrics(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            http_active_connections: self.http_active_connections.load(Ordering::Relaxed),
            http_bytes_transferred_in: self.http_bytes_transferred_in.load(Ordering::Relaxed),
            http_bytes_transferred_out: self.http_bytes_transferred_out.load(Ordering::Relaxed),
            tls_active_connections: self.tls_active_connections.load(Ordering::Relaxed),
            tls_bytes_transferred_in: self.tls_bytes_transferred_in.load(Ordering::Relaxed),
            tls_bytes_transferred_out: self.tls_bytes_transferred_out.load(Ordering::Relaxed),
            start_time: self.start_time.elapsed().as_secs(),
        }
    }
    
    // HTTP 수신 바이트 추가
    pub fn add_http_bytes_in(&self, bytes: u64) {
        self.http_bytes_transferred_in.fetch_add(bytes, Ordering::Relaxed);
    }
    
    // HTTP 송신 바이트 추가
    pub fn add_http_bytes_out(&self, bytes: u64) {
        self.http_bytes_transferred_out.fetch_add(bytes, Ordering::Relaxed);
    }
    
    // TLS 수신 바이트 추가
    pub fn add_tls_bytes_in(&self, bytes: u64) {
        self.tls_bytes_transferred_in.fetch_add(bytes, Ordering::Relaxed);
    }
    
    // TLS 송신 바이트 추가
    pub fn add_tls_bytes_out(&self, bytes: u64) {
        self.tls_bytes_transferred_out.fetch_add(bytes, Ordering::Relaxed);
    }
    
    // 연결 종료 처리
    pub fn connection_closed(&self, https_flag: bool) {
        if https_flag {
            self.tls_active_connections.fetch_sub(1, Ordering::Relaxed);
            debug!("HTTPS 연결 종료, 현재 활성 HTTPS 연결: {}", 
                  self.tls_active_connections.load(Ordering::Relaxed));
        } else {
            self.http_active_connections.fetch_sub(1, Ordering::Relaxed);
            debug!("HTTP 연결 종료, 현재 활성 HTTP 연결: {}", 
                  self.http_active_connections.load(Ordering::Relaxed));
        }
    }
    
    // 연결 시작 처리 - 추가
    pub fn connection_opened(&self, https_flag: bool) {
        if https_flag {
            self.tls_active_connections.fetch_add(1, Ordering::Relaxed);
            debug!("HTTPS 연결 시작, 현재 활성 HTTPS 연결: {}", 
                  self.tls_active_connections.load(Ordering::Relaxed));
        } else {
            self.http_active_connections.fetch_add(1, Ordering::Relaxed);
            debug!("HTTP 연결 시작, 현재 활성 HTTP 연결: {}", 
                  self.http_active_connections.load(Ordering::Relaxed));
        }
    }
}

struct MetricsSnapshot {
    http_active_connections: u64,
    http_bytes_transferred_in: u64,
    http_bytes_transferred_out: u64,
    tls_active_connections: u64,
    tls_bytes_transferred_in: u64,
    tls_bytes_transferred_out: u64,
    start_time: u64,
}

