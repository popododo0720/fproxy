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

    pub fn print_stats(&self) {
        let total_connection_opened = self.total_connection_opened.load(Ordering::Relaxed);
        let total_error_count = self.total_error_count.load(Ordering::Relaxed);
        let http_connections = self.http_connections.load(Ordering::Relaxed);
        let http_active_connections = self.http_active_connections.load(Ordering::Relaxed);
        let http_bytes_transferred_in = self.http_bytes_transferred_in.load(Ordering::Relaxed);
        let http_bytes_transferred_out = self.http_bytes_transferred_out.load(Ordering::Relaxed);
        let http_request_count = self.http_request_count.load(Ordering::Relaxed);
        let tls_connections = self.tls_connections.load(Ordering::Relaxed);
        let tls_active_connections = self.tls_active_connections.load(Ordering::Relaxed);
        let tls_bytes_transferred_in = self.tls_bytes_transferred_in.load(Ordering::Relaxed);
        let tls_bytes_transferred_out = self.tls_bytes_transferred_out.load(Ordering::Relaxed);
        let tls_request_count = self.tls_request_count.load(Ordering::Relaxed);
        let start_time = self.start_time.elapsed().as_secs();

        // 총 전송량 계산
        let total_in_mb = Self::bytes_to_mb(http_bytes_transferred_in + tls_bytes_transferred_in);
        let total_out_mb = Self::bytes_to_mb(http_bytes_transferred_out + tls_bytes_transferred_out);
        
        // HTTP 전송량
        let http_in_mb = Self::bytes_to_mb(http_bytes_transferred_in);
        let http_out_mb = Self::bytes_to_mb(http_bytes_transferred_out);
        
        // TLS 전송량
        let tls_in_mb = Self::bytes_to_mb(tls_bytes_transferred_in);
        let tls_out_mb = Self::bytes_to_mb(tls_bytes_transferred_out);

        info!("=== 프록시 서버 통계 ===");
        info!("가동 시간: {}초", start_time);
        info!("총 연결 수: {}", total_connection_opened);
        info!("총 HTTP 연결 수: {}", http_connections);
        info!("총 HTTPS 연결 수: {}", tls_connections);
        info!("총 활성 연결 수: {}", http_active_connections + tls_active_connections);
        info!("활성 HTTP 연결 수: {}", http_active_connections);
        info!("활성 HTTPS 연결 수: {}", tls_active_connections);
        
        info!("--- 전송 데이터 ---");
        info!("총 수신 데이터: {:.2} MB", total_in_mb);
        info!("총 송신 데이터: {:.2} MB", total_out_mb);
        info!("HTTP 수신 데이터: {:.2} MB", http_in_mb);
        info!("HTTP 송신 데이터: {:.2} MB", http_out_mb);
        info!("HTTPS 수신 데이터: {:.2} MB", tls_in_mb);
        info!("HTTPS 송신 데이터: {:.2} MB", tls_out_mb);
        
        info!("--- 요청 통계 ---");
        info!("HTTP 요청 수: {}", http_request_count);
        info!("HTTPS 요청 수: {}", tls_request_count);
        info!("총 요청 수: {}", http_request_count + tls_request_count);
        info!("오류 수: {}", total_error_count);
        info!("======================");
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
            debug!("TLS 요청 카운트 증가");
            self.tls_request_count.fetch_add(1, Ordering::Relaxed);
            self.tls_connections.fetch_add(1, Ordering::Relaxed);
            self.tls_active_connections.fetch_add(1, Ordering::Relaxed);
            // 모든 연결을 집계하기 위해 추가
            self.total_connection_opened();
        } else {
            debug!("HTTP 요청 카운트 증가");
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
            debug!("HTTP 수신 데이터 추가: {} bytes", bytes);
            let prev = self.http_bytes_transferred_in.fetch_add(bytes, Ordering::Relaxed);
            debug!("HTTP 수신 데이터 업데이트: {} -> {}", prev, prev + bytes);
        }
    }

    // HTTP 송신 데이터 추가
    pub fn add_http_bytes_out(&self, bytes: u64) {
        if bytes > 0 {
            debug!("HTTP 송신 데이터 추가: {} bytes", bytes);
            let prev = self.http_bytes_transferred_out.fetch_add(bytes, Ordering::Relaxed);
            debug!("HTTP 송신 데이터 업데이트: {} -> {}", prev, prev + bytes);
        }
    }

    // TLS 수신 데이터 추가
    pub fn add_tls_bytes_in(&self, bytes: u64) {
        if bytes > 0 {
            debug!("TLS 수신 데이터 추가: {} bytes", bytes);
            let prev = self.tls_bytes_transferred_in.fetch_add(bytes, Ordering::Relaxed);
            debug!("TLS 수신 데이터 업데이트: {} -> {}", prev, prev + bytes);
        }
    }

    // TLS 송신 데이터 추가
    pub fn add_tls_bytes_out(&self, bytes: u64) {
        if bytes > 0 {
            debug!("TLS 송신 데이터 추가: {} bytes", bytes);
            let prev = self.tls_bytes_transferred_out.fetch_add(bytes, Ordering::Relaxed);
            debug!("TLS 송신 데이터 업데이트: {} -> {}", prev, prev + bytes);
        }
    }
    
    // 연결 종료시 카운트 감소
    pub fn connection_closed(&self, https_flag: bool) {
        if https_flag {
            debug!("TLS 연결 종료");
            self.tls_active_connections.fetch_sub(1, Ordering::Relaxed);
        } else {
            debug!("HTTP 연결 종료");
            self.http_active_connections.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

