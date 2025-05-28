use std::sync::atomic::{ AtomicU64, Ordering };
use std::time::{ Duration, Instant };
use std::sync::{ Arc };

use tokio::time;
use log::{ info, debug };
use once_cell::sync::Lazy;

use crate::constants::*;

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
        bytes as f64 / (1024.0 * 1024.0)
    }

    pub fn print_stats(&self) {
        // 모든 카운터 값을 한 번에 로드 (메모리 순서 일관성 유지)
        let metrics = self.load_all_metrics();
        
        // 데이터 전송량 계산
        let transfer_stats = self.calculate_transfer_stats(&metrics);

        // 로그 출력
        self.log_basic_stats(&metrics);
        self.log_transfer_stats(&transfer_stats);
        
        info!("======================");
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
        info!("총 활성 연결 수: {}", 
            metrics.http_active_connections + metrics.tls_active_connections);
        info!("활성 HTTP 연결 수: {}", metrics.http_active_connections);
        info!("활성 HTTPS 연결 수: {}", metrics.tls_active_connections);
    }
    
    // 전송 통계 로깅
    fn log_transfer_stats(&self, stats: &TransferStats) {
        info!("=== 데이터 전송 통계 ===");
        info!("총 수신: {:.2} MB", stats.total_in_mb);
        info!("총 송신: {:.2} MB", stats.total_out_mb);
        info!("HTTP 수신: {:.2} MB", stats.http_in_mb);
        info!("HTTP 송신: {:.2} MB", stats.http_out_mb);
        info!("HTTPS 수신: {:.2} MB", stats.tls_in_mb);
        info!("HTTPS 송신: {:.2} MB", stats.tls_out_mb);
    }
    
    // 요청 카운트 증가 - 빈 함수로 유지 (호환성)
    pub fn increment_request_count(&self, _https_flag: bool) {
        // DB에 저장하도록 변경되었으므로 여기서는 아무 작업도 하지 않음
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

struct TransferStats {
    total_in_mb: f64,
    total_out_mb: f64,
    http_in_mb: f64,
    http_out_mb: f64,
    tls_in_mb: f64,
    tls_out_mb: f64,
}

