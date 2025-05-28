use std::sync::atomic::{ AtomicU64, Ordering };
use std::time::{ Duration, Instant };
use std::sync::{ Arc, Mutex };

use tokio::time;
use log::{ info, error, debug };
use once_cell::sync::Lazy;

use crate::constants::*;

// 평균 응답 시간 저장용 구조체
#[derive(Debug, Clone, Copy)]
struct ResponseTimeStats {
    count: u64,
    avg_time_ms: f64,
    min_time_ms: u64,
    max_time_ms: u64,
}

impl ResponseTimeStats {
    /// 새 통계 인스턴스 생성
    fn new() -> Self {
        Self::default()
    }
    
    /// 새 응답 시간을 추가하고 통계 업데이트
    fn add_response_time(&mut self, duration_ms: u64) -> &mut Self {
        // 이전 합계 계산 (평균 * 개수)
        let prev_total = self.count as f64 * self.avg_time_ms;
        
        // 카운트 증가
        self.count += 1;
        
        // 새 평균 계산
        self.avg_time_ms = (prev_total + duration_ms as f64) / self.count as f64;
        
        // 최소값 업데이트 (첫 번째 요청이거나 더 작은 값인 경우)
        if self.count == 1 || duration_ms < self.min_time_ms {
            self.min_time_ms = duration_ms;
        }
        
        // 최대값 업데이트
        self.max_time_ms = self.max_time_ms.max(duration_ms);
        
        self
    }
    
    /// 현재 통계 정보를 로그에 출력
    fn log_current_stats(&self, duration_ms: u64) {
        debug!("응답 시간 통계 업데이트: count={}, 새 응답시간={}ms, 평균={:.2}ms, 최소={}ms, 최대={}ms", 
            self.count, duration_ms, self.avg_time_ms, self.min_time_ms, self.max_time_ms);
    }
    
    /// 로그에 자세한 통계 정보 출력
    fn log_detailed_stats(&self) {
        info!("--- 응답 시간 통계 ---");
        info!("총 처리 요청 수: {}", self.count);
        info!("평균 응답 시간: {:.2} ms", self.avg_time_ms);
        info!("최소 응답 시간: {} ms", self.min_time_ms);
        info!("최대 응답 시간: {} ms", self.max_time_ms);
    }
    
    /// 응답 시간이 유효한지 확인
    fn is_valid_duration(duration_ms: u64) -> bool {
        // 0ms 응답은 연결 오류 또는 타임아웃일 가능성이 높음
        if duration_ms == 0 {
            debug!("응답 시간이 0ms로 측정됨, 통계에서 제외합니다");
            return false;
        } 
        
        // 30초 초과 응답은 long polling 또는 웹소켓일 가능성이 높음
        if duration_ms > 30000 {
            debug!("응답 시간이 {}ms로 비정상적으로 김, 통계에서 제외합니다", duration_ms);
            return false;
        }
        
        true
    }
}

impl Default for ResponseTimeStats {
    fn default() -> Self {
        Self {
            count: 0,
            avg_time_ms: 0.0,
            min_time_ms: 0,
            max_time_ms: 0,
        }
    }
}

// 응답 시간 통계를 위한 전역 변수
static RESPONSE_STATS: Lazy<Mutex<ResponseTimeStats>> = Lazy::new(|| {
    Mutex::new(ResponseTimeStats::new())
});

// 전역 메트릭스 인스턴스를 위한 Lazy 정적 변수
static METRICS_INSTANCE: Lazy<Arc<Metrics>> = Lazy::new(|| {
    let metrics = Arc::new(Metrics::new_internal());
    
    // 주기적인 통계 로깅 설정
    let metrics_clone = Arc::clone(&metrics);
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(METRICS_INTERVAL_SECS));
        loop {
            interval.tick().await;
            metrics_clone.print_stats();
        }
    });
    
    metrics
});

pub struct Metrics {
    total_connection_opened: AtomicU64,
    total_error_count: AtomicU64,
    http_connections: AtomicU64,
    http_active_connections: AtomicU64,
    http_bytes_transferred_in: AtomicU64,
    http_bytes_transferred_out: AtomicU64,
    http_request_count: AtomicU64,
    tls_connections: AtomicU64,
    tls_active_connections: AtomicU64,
    tls_bytes_transferred_in: AtomicU64,
    tls_bytes_transferred_out: AtomicU64,
    tls_request_count: AtomicU64,
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
            total_connection_opened: AtomicU64::new(0),
            total_error_count: AtomicU64::new(0),
            http_connections: AtomicU64::new(0),
            http_active_connections: AtomicU64::new(0),
            http_bytes_transferred_in: AtomicU64::new(0),
            http_bytes_transferred_out: AtomicU64::new(0),
            http_request_count: AtomicU64::new(0),
            tls_connections: AtomicU64::new(0),
            tls_active_connections: AtomicU64::new(0),
            tls_bytes_transferred_in: AtomicU64::new(0),
            tls_bytes_transferred_out: AtomicU64::new(0),
            tls_request_count: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    // 바이트를 MB로 변환
    fn bytes_to_mb(bytes: u64) -> f64 {
        bytes as f64 / (1024.0 * 1024.0)
    }

    // 응답 시간 통계 업데이트
    pub fn update_response_time_stats(&self, duration_ms: u64) {
        // 유효하지 않은 응답 시간은 처리하지 않음
        if !ResponseTimeStats::is_valid_duration(duration_ms) {
            return;
        }

        // 응답 시간 통계 업데이트
        RESPONSE_STATS
            .lock()
            .map(|mut stats| {
                stats.add_response_time(duration_ms)
                     .log_current_stats(duration_ms);
            })
            .unwrap_or_else(|_| {
                error!("응답 시간 통계 업데이트 실패: 락 획득 실패");
            });
    }

    pub fn print_stats(&self) {
        // 모든 카운터 값을 한 번에 로드 (메모리 순서 일관성 유지)
        let metrics = self.load_all_metrics();
        
        // 데이터 전송량 계산
        let transfer_stats = self.calculate_transfer_stats(&metrics);
        
        // 응답 시간 통계 가져오기
        let response_stats = RESPONSE_STATS.lock().ok().map(|stats| *stats);

        // 로그 출력
        self.log_basic_stats(&metrics);
        self.log_transfer_stats(&transfer_stats);
        self.log_request_stats(&metrics);
        
        // 응답 시간 통계 출력
        if let Some(stats) = response_stats {
            if stats.count > 0 {
                self.log_response_time_stats(&stats);
            }
        }
        
        info!("======================");
    }
    
    // 모든 메트릭스 데이터를 구조체로 로드
    fn load_all_metrics(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            total_connection_opened: self.total_connection_opened.load(Ordering::Relaxed),
            total_error_count: self.total_error_count.load(Ordering::Relaxed),
            http_connections: self.http_connections.load(Ordering::Relaxed),
            http_active_connections: self.http_active_connections.load(Ordering::Relaxed),
            http_bytes_transferred_in: self.http_bytes_transferred_in.load(Ordering::Relaxed),
            http_bytes_transferred_out: self.http_bytes_transferred_out.load(Ordering::Relaxed),
            http_request_count: self.http_request_count.load(Ordering::Relaxed),
            tls_connections: self.tls_connections.load(Ordering::Relaxed),
            tls_active_connections: self.tls_active_connections.load(Ordering::Relaxed),
            tls_bytes_transferred_in: self.tls_bytes_transferred_in.load(Ordering::Relaxed),
            tls_bytes_transferred_out: self.tls_bytes_transferred_out.load(Ordering::Relaxed),
            tls_request_count: self.tls_request_count.load(Ordering::Relaxed),
            start_time: self.start_time.elapsed().as_secs(),
        }
    }
    
    // 전송 통계 계산
    fn calculate_transfer_stats(&self, metrics: &MetricsSnapshot) -> TransferStats {
        TransferStats {
            total_in_mb: Self::bytes_to_mb(
                metrics.http_bytes_transferred_in + metrics.tls_bytes_transferred_in
            ),
            total_out_mb: Self::bytes_to_mb(
                metrics.http_bytes_transferred_out + metrics.tls_bytes_transferred_out
            ),
            http_in_mb: Self::bytes_to_mb(metrics.http_bytes_transferred_in),
            http_out_mb: Self::bytes_to_mb(metrics.http_bytes_transferred_out),
            tls_in_mb: Self::bytes_to_mb(metrics.tls_bytes_transferred_in),
            tls_out_mb: Self::bytes_to_mb(metrics.tls_bytes_transferred_out),
        }
    }
    
    // 기본 통계 로깅
    fn log_basic_stats(&self, metrics: &MetricsSnapshot) {
        info!("=== 프록시 서버 통계 ===");
        info!("가동 시간: {}초", metrics.start_time);
        info!("총 연결 수: {}", metrics.total_connection_opened);
        info!("총 HTTP 연결 수: {}", metrics.http_connections);
        info!("총 HTTPS 연결 수: {}", metrics.tls_connections);
        info!("총 활성 연결 수: {}", 
            metrics.http_active_connections + metrics.tls_active_connections);
        info!("활성 HTTP 연결 수: {}", metrics.http_active_connections);
        info!("활성 HTTPS 연결 수: {}", metrics.tls_active_connections);
    }
    
    // 데이터 전송 통계 로깅
    fn log_transfer_stats(&self, stats: &TransferStats) {
        info!("--- 전송 데이터 ---");
        info!("총 수신 데이터: {:.2} MB", stats.total_in_mb);
        info!("총 송신 데이터: {:.2} MB", stats.total_out_mb);
        info!("HTTP 수신 데이터: {:.2} MB", stats.http_in_mb);
        info!("HTTP 송신 데이터: {:.2} MB", stats.http_out_mb);
        info!("HTTPS 수신 데이터: {:.2} MB", stats.tls_in_mb);
        info!("HTTPS 송신 데이터: {:.2} MB", stats.tls_out_mb);
    }
    
    // 요청 통계 로깅
    fn log_request_stats(&self, metrics: &MetricsSnapshot) {
        info!("--- 요청 통계 ---");
        info!("HTTP 요청 수: {}", metrics.http_request_count);
        info!("HTTPS 요청 수: {}", metrics.tls_request_count);
        info!("총 요청 수: {}", 
            metrics.http_request_count + metrics.tls_request_count);
        info!("오류 수: {}", metrics.total_error_count);
    }

    // 커넥션 생성시 카운트 증가
    pub fn total_connection_opened(&self) {
        self.total_connection_opened.fetch_add(1, Ordering::Relaxed);
    }

    // 에러 카운트
    pub fn record_error(&self) {
        self.total_error_count.fetch_add(1, Ordering::Relaxed);
    }

    // 요청카운트 증가
    pub fn increment_request_count(&self, https_flag: bool) {
        if https_flag {
            self.tls_request_count.fetch_add(1, Ordering::Relaxed);
            self.tls_connections.fetch_add(1, Ordering::Relaxed);
            self.tls_active_connections.fetch_add(1, Ordering::Relaxed);
            // 모든 연결을 집계하기 위해 추가
            self.total_connection_opened();
        } else {
            self.http_request_count.fetch_add(1, Ordering::Relaxed);
            self.http_connections.fetch_add(1, Ordering::Relaxed);
            self.http_active_connections.fetch_add(1, Ordering::Relaxed);
            // 모든 연결을 집계하기 위해 추가
            self.total_connection_opened();
        }
    }

    // HTTP 수신 데이터 추가
    pub fn add_http_bytes_in(&self, bytes: u64) {
        if bytes > 0 {
            self.http_bytes_transferred_in.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    // HTTP 송신 데이터 추가
    pub fn add_http_bytes_out(&self, bytes: u64) {
        if bytes > 0 {
            self.http_bytes_transferred_out.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    // TLS 수신 데이터 추가
    pub fn add_tls_bytes_in(&self, bytes: u64) {
        if bytes > 0 {
            self.tls_bytes_transferred_in.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    // TLS 송신 데이터 추가
    pub fn add_tls_bytes_out(&self, bytes: u64) {
        if bytes > 0 {
            self.tls_bytes_transferred_out.fetch_add(bytes, Ordering::Relaxed);
        }
    }
    
    // 연결 종료시 카운트 감소
    pub fn connection_closed(&self, https_flag: bool) {
        if https_flag {
            self.tls_active_connections.fetch_sub(1, Ordering::Relaxed);
        } else {
            self.http_active_connections.fetch_sub(1, Ordering::Relaxed);
        }
    }

    // 응답 시간 통계 로깅
    fn log_response_time_stats(&self, stats: &ResponseTimeStats) {
        stats.log_detailed_stats();
    }
}

// 메트릭스 스냅샷 데이터 구조체
#[derive(Debug)]
struct MetricsSnapshot {
    total_connection_opened: u64,
    total_error_count: u64,
    http_connections: u64,
    http_active_connections: u64,
    http_bytes_transferred_in: u64,
    http_bytes_transferred_out: u64,
    http_request_count: u64,
    tls_connections: u64,
    tls_active_connections: u64,
    tls_bytes_transferred_in: u64,
    tls_bytes_transferred_out: u64,
    tls_request_count: u64,
    start_time: u64,
}

// 전송 통계 구조체
#[derive(Debug)]
struct TransferStats {
    total_in_mb: f64,
    total_out_mb: f64,
    http_in_mb: f64,
    http_out_mb: f64,
    tls_in_mb: f64,
    tls_out_mb: f64,
}

