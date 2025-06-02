// SQL 쿼리 모듈 (테이블별 SQL 쿼리 관리)
pub mod request_logs;
pub mod proxy_stats;
pub mod proxy_stats_hourly;
pub mod partition;
pub mod domain_blocks;
pub mod domain_pattern_blocks;
pub mod response_logs;

// 버퍼 크기
pub const BUFFER_SIZE_SMALL: usize = 64 * 1024;   // 64KB
pub const BUFFER_SIZE_MEDIUM: usize = 256 * 1024;    // 256KB
pub const BUFFER_SIZE_LARGE: usize = 1024 * 1024;    // 1MB

// 버퍼 풀 크기
pub const SMALL_POOL_SIZE: usize = 1000;
pub const MEDIUM_POOL_SIZE: usize = 500;
pub const LARGE_POOL_SIZE: usize = 100;

// 적응형 버퍼링 시스템 설정
pub const BUFFER_ADJUSTMENT_INTERVAL_SECS: u64 = 30;  // 버퍼 크기 조정 간격
pub const BUFFER_USAGE_THRESHOLD_HIGH: f64 = 0.8;     // 버퍼 풀 확장 임계값 (80%)
pub const BUFFER_USAGE_THRESHOLD_LOW: f64 = 0.3;      // 버퍼 풀 축소 임계값 (30%)
pub const BUFFER_POOL_ADJUSTMENT_RATE: f64 = 0.2;     // 버퍼 풀 조정 비율 (20%)

pub const BUFFER_STATS_INTERVAL_SECS: u64 = 30;     // 버퍼 통계 출력

pub const TCP_NODELAY: bool = true;
pub const TCP_QUICKACK: bool = true;  // TCP QUICKACK 활성화

// LRU 캐시 크기
pub const CERT_CACHE_SIZE: usize = 1000;         // 인증서 캐시 크기
pub const TLS_SESSION_CACHE_SIZE: usize = 5000;  // TLS 세션 캐시 크기
pub const ACL_CACHE_SIZE: usize = 10000;         // ACL 결과 캐시 크기

// 루트 CA 인증서 파일 경로
pub const CA_CERT_FILE: &str = "ssl/ca_cert.pem";
pub const CA_KEY_FILE: &str = "ssl/ca_key.pem";
pub const CA_CERT_CRT_FILE: &str = "ssl/ca_cert.crt";

// 로그 배치 크기 및 플러시 간격
pub const LOG_BATCH_SIZE: usize = 500;
pub const LOG_FLUSH_INTERVAL_MS: u64 = 1000;
pub const LOG_CHANNEL_SIZE: usize = 10000;    // 로그 채널 크기

// 워커 풀 설정
pub const LOG_WORKER_COUNT: usize = 16;  // 로그 워커 스레드 수 (기존 8에서 증가)
pub const LOG_HIGH_PRIORITY_QUEUE_SIZE: usize = 8000;  // 고우선순위 로그 큐 크기
pub const LOG_MEDIUM_PRIORITY_QUEUE_SIZE: usize = 12000;  // 중간우선순위 로그 큐 크기
pub const LOG_LOW_PRIORITY_QUEUE_SIZE: usize = 15000;  // 저우선순위 로그 큐 크기

// 로그 처리 타임아웃 (ms)
pub const LOG_PROCESSING_TIMEOUT_MS: u64 = 1000;  // 로그 처리 타임아웃
pub const LOG_DB_OPERATION_TIMEOUT_MS: u64 = 2000;  // DB 작업 타임아웃

// 로그 파티션 관리
pub const LOG_PARTITION_CHECK_INTERVAL_HOURS: u64 = 24;  // 파티션 체크 주기 (시간)
