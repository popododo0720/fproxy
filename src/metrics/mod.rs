use std::sync::atomic::{ AtomicU64, Ordering };
use std::time::{ Duration, Instant };
use std::sync::{ Arc };

use tokio::time;
use log::{ info, debug, error, warn };
use once_cell::sync::Lazy;
use chrono::Utc;
use chrono::Datelike;
use chrono::Timelike;

use crate::db;
use crate::constants::proxy_stats;
use crate::constants::proxy_stats_hourly;

// 전역 메트릭스 인스턴스를 위한 Lazy 정적 변수
static METRICS_INSTANCE: Lazy<Arc<Metrics>> = Lazy::new(|| {
    let metrics = Arc::new(Metrics::new_internal());
    
    // 주기적인 통계 로깅 설정 (비활성화)
    // 주기적인 통계 로깅 코드 제거
    
    // DB에서 마지막 메트릭스 값 로드 시도
    let metrics_clone = Arc::clone(&metrics);
    tokio::spawn(async move {
        // DB에서 마지막 메트릭스 값 로드
        if let Err(e) = metrics_clone.load_last_metrics_from_db().await {
            error!("DB에서 마지막 메트릭스 로드 실패: {}", e);
        } else {
            info!("DB에서 마지막 메트릭스 로드 완료");
        }
    });
    
    // 시간별 통계 테이블 초기화
    let _metrics_clone = Arc::clone(&metrics);
    tokio::spawn(async move {
        // 시간별 통계 테이블 생성 확인
        match db::query::QueryExecutor::get_instance().await {
            Ok(executor) => {
                if let Err(e) = Metrics::ensure_hourly_stats_table(&executor).await {
                    error!("시간별 통계 테이블 초기화 실패: {}", e);
                } else {
                    info!("시간별 통계 테이블 초기화 완료");
                }
            },
            Err(e) => {
                error!("쿼리 실행기 가져오기 실패: {}", e);
            }
        }
    });
    
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
            
            // 매 시간 시작에 카운터 리셋
            let now = Utc::now();
            if now.minute() == 0 && now.second() == 0 {
                debug!("매 시간 시작: 전송량 카운터 리셋");
                metrics_clone.reset_transfer_counters();
            }
            
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
    last_reset_time: std::sync::RwLock<Instant>,  // 마지막 리셋 시간
}

impl Metrics {
    // 싱글톤 인스턴스를 반환
    pub fn new() -> Arc<Self> {
        Arc::clone(&METRICS_INSTANCE)
    }
    
    // HTTP 세션 카운터 증가
    pub fn increment_http_session_count(&self) {
        // 실제 구현 생략 - 사용하지 않는 메서드지만 인터페이스 유지
    }
    
    // HTTP 요청 카운터 증가
    pub fn increment_http_request_count(&self) {
        // 실제 구현 생략 - 사용하지 않는 메서드지만 인터페이스 유지
    }
    
    // HTTP 응답 카운터 증가
    pub fn increment_http_response_count(&self) {
        // 실제 구현 생략 - 사용하지 않는 메서드지만 인터페이스 유지
    }
    
    // GET 요청 카운터 증가
    pub fn increment_get_request_count(&self) {
        // 실제 구현 생략 - 사용하지 않는 메서드지만 인터페이스 유지
    }
    
    // POST 요청 카운터 증가
    pub fn increment_post_request_count(&self) {
        // 실제 구현 생략 - 사용하지 않는 메서드지만 인터페이스 유지
    }
    
    // 오류 카운터 증가
    pub fn increment_error_count(&self) {
        // 실제 구현 생략 - 사용하지 않는 메서드지만 인터페이스 유지
    }
    
    // 응답 시간 기록
    pub fn record_response_time(&self, _response_time: u64) {
        // 실제 구현 생략 - 사용하지 않는 메서드지만 인터페이스 유지
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
            last_reset_time: std::sync::RwLock::new(Instant::now()),  // 초기화
        }
    }

    // 바이트를 MB로 변환
    fn bytes_to_mb(bytes: u64) -> f64 {
        let mb = bytes as f64 / (1024.0 * 1024.0);
        (mb * 1000.0).round() / 1000.0  // 소수점 세 자리로 제한
    }
    
    // 전송량 카운터 리셋
    fn reset_transfer_counters(&self) {
        // 리셋 전에 현재 값을 시간별 통계로 저장
        let metrics = self.load_all_metrics();
        let metrics_clone = metrics.clone(); // 비동기 작업을 위한 복제
        
        // 비동기 작업으로 시간별 통계 저장
        tokio::spawn(async move {
            if let Err(e) = Self::save_hourly_stats(metrics_clone).await {
                error!("시간별 통계 저장 실패: {}", e);
            }
        });
        
        // 카운터 리셋
        self.http_bytes_transferred_in.store(0, Ordering::Relaxed);
        self.http_bytes_transferred_out.store(0, Ordering::Relaxed);
        self.tls_bytes_transferred_in.store(0, Ordering::Relaxed);
        self.tls_bytes_transferred_out.store(0, Ordering::Relaxed);
        
        if let Ok(mut last_reset) = self.last_reset_time.write() {
            *last_reset = Instant::now();
        }
        
        info!("전송량 카운터가 리셋되었습니다.");
    }
    
    // 시간별 통계 저장
    async fn save_hourly_stats(metrics: MetricsSnapshot) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 쿼리 실행기 가져오기
        let executor = match db::query::QueryExecutor::get_instance().await {
            Ok(executor) => executor,
            Err(e) => {
                error!("쿼리 실행기 가져오기 실패: {}", e);
                return Err(e);
            }
        };
        
        // 시간별 통계 테이블 확인 및 생성
        if let Err(e) = Self::ensure_hourly_stats_table(&executor).await {
            error!("시간별 통계 테이블 생성 실패: {}", e);
            return Err(e);
        }
        
        // 현재 시간 (시간 단위로 절사)
        let hour_start = Utc::now()
            .with_minute(0).unwrap()
            .with_second(0).unwrap()
            .with_nanosecond(0).unwrap();
        
        // 바이트를 메가바이트로 변환
        let http_bytes_in_mb = Self::bytes_to_mb(metrics.http_bytes_transferred_in);
        let http_bytes_out_mb = Self::bytes_to_mb(metrics.http_bytes_transferred_out);
        let tls_bytes_in_mb = Self::bytes_to_mb(metrics.tls_bytes_transferred_in);
        let tls_bytes_out_mb = Self::bytes_to_mb(metrics.tls_bytes_transferred_out);
        
        // 쿼리 파라미터
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
            &hour_start,
            &(metrics.http_active_connections as f64), // 평균 연결 수로 사용
            &http_bytes_in_mb,
            &http_bytes_out_mb,
            &(metrics.tls_active_connections as f64), // 평균 연결 수로 사용
            &tls_bytes_in_mb,
            &tls_bytes_out_mb,
            &(metrics.start_time as i64)
        ];
        
        // 쿼리 실행
        match executor.execute_query(proxy_stats_hourly::INSERT_STATS, params).await {
            Ok(_) => {
                info!("시간별 통계 저장 완료: {}", hour_start);
                Ok(())
            },
            Err(e) => {
                error!("시간별 통계 저장 실패: {}", e);
                Err(e.into())
            }
        }
    }
    
    // 시간별 통계 테이블 생성 확인
    async fn ensure_hourly_stats_table(executor: &db::query::QueryExecutor) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 테이블 존재 여부 확인
        let table_exists = match executor.query_one(proxy_stats_hourly::CHECK_TABLE_EXISTS, &[], |row| Ok(row.get::<_, bool>(0))).await {
            Ok(exists) => exists,
            Err(e) => {
                error!("테이블 존재 여부 확인 실패: {}", e);
                false
            }
        };
        
        if !table_exists {
            // 테이블 생성
            if let Err(e) = executor.execute_query(proxy_stats_hourly::CREATE_TABLE, &[]).await {
                error!("proxy_stats_hourly 테이블 생성 실패: {}", e);
                return Err(e);
            }
            
            // 인덱스 생성
            for index_query in proxy_stats_hourly::CREATE_INDICES.iter() {
                if let Err(e) = executor.execute_query(index_query, &[]).await {
                    warn!("proxy_stats_hourly 인덱스 생성 실패: {}", e);
                    // 인덱스 생성 실패는 치명적이지 않으므로 계속 진행
                }
            }
            
            info!("proxy_stats_hourly 테이블이 성공적으로 생성되었습니다.");
        }
        
        Ok(())
    }
    
    // 마지막 리셋 이후 경과 시간(초)
    fn seconds_since_last_reset(&self) -> u64 {
        if let Ok(last_reset) = self.last_reset_time.read() {
            last_reset.elapsed().as_secs()
        } else {
            0
        }
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
        let table_exists = match executor.query_one(proxy_stats::CHECK_TABLE_EXISTS, &[], |row| Ok(row.get::<_, bool>(0))).await {
            Ok(exists) => exists,
            Err(e) => {
                error!("테이블 존재 여부 확인 실패: {}", e);
                false
            }
        };
        
        if !table_exists {
            // 테이블이 존재하지 않으면 새로 생성
            debug!("proxy_stats 테이블이 존재하지 않습니다. 새로 생성합니다.");
            
            // 테이블 생성 실행
            if let Err(e) = executor.execute_query(proxy_stats::CREATE_TABLE, &[]).await {
                error!("proxy_stats 테이블 생성 실패: {}", e);
                return Err(e);
            }
            
            // 인덱스 생성 실행
            for index_query in proxy_stats::CREATE_INDICES.iter() {
                if let Err(e) = executor.execute_query(index_query, &[]).await {
                    error!("proxy_stats 인덱스 생성 실패: {}", e);
                    // 인덱스 생성 실패는 치명적이지 않으므로 계속 진행
                    warn!("proxy_stats 인덱스 생성 실패했지만 계속 진행합니다");
                }
            }
            
            info!("proxy_stats 테이블이 성공적으로 생성되었습니다.");
        } else {
            debug!("proxy_stats 테이블이 이미 존재합니다.");
        }
        
        // 오늘과 내일 파티션 생성
        let today = chrono::Local::now().date_naive();
        let tomorrow = today + chrono::Duration::days(1);
        
        // 오늘 파티션 생성
        match Self::create_stats_partition(&executor, today, tomorrow).await {
            Ok(partition_name) => {
                info!("오늘 파티션 생성 완료: {}", partition_name);
            },
            Err(e) => {
                error!("오늘 파티션 생성 실패: {}", e);
            }
        }
        
        // 내일 파티션 생성
        match Self::create_stats_partition(&executor, tomorrow, tomorrow + chrono::Duration::days(1)).await {
            Ok(partition_name) => {
                info!("내일 파티션 생성 완료: {}", partition_name);
            },
            Err(e) => {
                error!("내일 파티션 생성 실패: {}", e);
            }
        }
        
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
        let partition_exists = match executor.query_one(proxy_stats::CHECK_PARTITION_EXISTS, &[&partition_name], |row| Ok(row.get::<_, bool>(0))).await {
            Ok(exists) => exists,
            Err(e) => {
                error!("파티션 존재 여부 확인 실패: {}", e);
                false
            }
        };
        
        if !partition_exists {
            // 파티션 생성 쿼리
            let create_partition_query = format!(
                "{}",
                proxy_stats::CREATE_PARTITION_FORMAT
            ).replace("{}", &partition_name)
             .replace("{}", &start_date.to_string())
             .replace("{}", &end_date.to_string());
            
            // 파티션 생성 실행
            if let Err(e) = executor.execute_query(&create_partition_query, &[]).await {
                error!("파티션 생성 실패: {}", e);
                return Err(e.into());
            }
            
            // 파티션별 인덱스 생성
            let create_index_query = format!(
                "{}",
                proxy_stats::CREATE_PARTITION_INDEX_FORMAT
            ).replace("{}", &partition_name)
             .replace("{}", &partition_name);
            
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
        
        // 현재 시간
        let now = Utc::now();
        
        // 바이트를 메가바이트로 변환하고 소수점 세 자리로 제한
        let http_bytes_in_mb = (Self::bytes_to_mb(metrics.http_bytes_transferred_in) * 1000.0).round() / 1000.0;
        let http_bytes_out_mb = (Self::bytes_to_mb(metrics.http_bytes_transferred_out) * 1000.0).round() / 1000.0;
        let tls_bytes_in_mb = (Self::bytes_to_mb(metrics.tls_bytes_transferred_in) * 1000.0).round() / 1000.0;
        let tls_bytes_out_mb = (Self::bytes_to_mb(metrics.tls_bytes_transferred_out) * 1000.0).round() / 1000.0;
        
        // 마지막 리셋 이후 경과 시간
        let seconds_since_reset = self.seconds_since_last_reset() as i64;
        
        // 쿼리 파라미터 (메가바이트 단위로 저장)
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
            &now,
            &(metrics.http_active_connections as i64),
            &http_bytes_in_mb,  // 메가바이트 단위
            &http_bytes_out_mb, // 메가바이트 단위
            &(metrics.tls_active_connections as i64),
            &tls_bytes_in_mb,   // 메가바이트 단위
            &tls_bytes_out_mb,  // 메가바이트 단위
            &(metrics.start_time as i64),
            &seconds_since_reset // 마지막 리셋 이후 경과 시간
        ];
        
        // 쿼리 실행
        match executor.execute_query(proxy_stats::INSERT_STATS, params).await {
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
                            info!("오늘 파티션 생성 완료: {}", partition_name);
                            
                            // 내일 파티션도 미리 생성
                            match Self::create_stats_partition(&executor, tomorrow, tomorrow + chrono::Duration::days(1)).await {
                                Ok(tomorrow_partition) => {
                                    info!("내일 파티션 생성 완료: {}", tomorrow_partition);
                                },
                                Err(e) => {
                                    warn!("내일 파티션 생성 실패: {}", e);
                                }
                            }
                            
                            // 파티션 생성 후 다시 시도
                            match executor.execute_query(proxy_stats::INSERT_STATS, params).await {
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

    // DB에서 마지막 메트릭스 값 로드
    async fn load_last_metrics_from_db(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 쿼리 실행기 가져오기
        let executor = match db::query::QueryExecutor::get_instance().await {
            Ok(executor) => executor,
            Err(e) => {
                debug!("쿼리 실행기 가져오기 실패: {}. 초기 상태로 시작합니다.", e);
                return Ok(()); // 초기 상태로 시작
            }
        };
        
        // 마지막 메트릭스 값 조회
        match executor.query_one(proxy_stats::SELECT_LAST_METRICS, &[], |row| {
            let http_in: f64 = row.get(1);
            let http_out: f64 = row.get(2);
            let tls_in: f64 = row.get(4);
            let tls_out: f64 = row.get(5);
            
            // MB에서 바이트로 변환 (데이터베이스에는 MB 단위로 저장됨)
            let http_in_bytes = (http_in * 1024.0 * 1024.0) as u64;
            let http_out_bytes = (http_out * 1024.0 * 1024.0) as u64;
            let tls_in_bytes = (tls_in * 1024.0 * 1024.0) as u64;
            let tls_out_bytes = (tls_out * 1024.0 * 1024.0) as u64;
            
            // 결과를 AtomicU64에 저장
            Ok((
                http_in_bytes,
                http_out_bytes,
                tls_in_bytes,
                tls_out_bytes
            ))
        }).await {
            Ok((http_in, http_out, tls_in, tls_out)) => {
                // 값을 AtomicU64에 저장
                // 활성 연결 수는 프로그램 재시작 시 0으로 시작 (모든 연결이 종료되기 때문)
                self.http_active_connections.store(0, Ordering::SeqCst);
                self.http_bytes_transferred_in.store(http_in, Ordering::SeqCst);
                self.http_bytes_transferred_out.store(http_out, Ordering::SeqCst);
                self.tls_active_connections.store(0, Ordering::SeqCst);
                self.tls_bytes_transferred_in.store(tls_in, Ordering::SeqCst);
                self.tls_bytes_transferred_out.store(tls_out, Ordering::SeqCst);
                
                info!("DB에서 로드된 메트릭스: HTTP in: {:.2} MB, out: {:.2} MB, TLS in: {:.2} MB, out: {:.2} MB",
                    Self::bytes_to_mb(http_in),
                    Self::bytes_to_mb(http_out),
                    Self::bytes_to_mb(tls_in),
                    Self::bytes_to_mb(tls_out)
                );
                
                Ok(())
            },
            Err(e) => {
                // 데이터가 없는 경우는 정상적인 상황으로 처리
                if e.to_string().contains("no rows") {
                    debug!("DB에 저장된 메트릭스 데이터가 없습니다. 초기 상태로 시작합니다.");
                    return Ok(());
                }
                
                debug!("DB에서 마지막 메트릭스 로드 실패: {}. 초기 상태로 시작합니다.", e);
                Ok(()) // 에러 상황에서도 초기 상태로 시작
            }
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

// Clone 트레이트 구현
impl Clone for MetricsSnapshot {
    fn clone(&self) -> Self {
        Self {
            http_active_connections: self.http_active_connections,
            http_bytes_transferred_in: self.http_bytes_transferred_in,
            http_bytes_transferred_out: self.http_bytes_transferred_out,
            tls_active_connections: self.tls_active_connections,
            tls_bytes_transferred_in: self.tls_bytes_transferred_in,
            tls_bytes_transferred_out: self.tls_bytes_transferred_out,
            start_time: self.start_time,
        }
    }
}

