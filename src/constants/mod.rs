// 버퍼 크기
pub const BUFFER_SIZE_SMALL: usize = 64 * 1024;   // 64KB
pub const BUFFER_SIZE_MEDIUM: usize = 256 * 1024;    // 256KB
pub const BUFFER_SIZE_LARGE: usize = 1024 * 1024;    // 1MB

// 버퍼 풀 크기
pub const SMALL_POOL_SIZE: usize = 2000;
pub const MEDIUM_POOL_SIZE: usize = 1000;
pub const LARGE_POOL_SIZE: usize = 200;

pub const METRICS_INTERVAL_SECS: u64 = 5;         // 메트릭 출력
pub const BUFFER_STATS_INTERVAL_SECS: u64 = 5;     // 버퍼 통계 출력

pub const TCP_NODELAY: bool = true;
// pub const TCP_KEEPALIVE_SECS: u32 = 60;
// pub const TCP_FASTOPEN: bool = true;
// pub const TCP_QUICKACK: bool = true;
// pub const USE_SPLICE: bool = true; // 제로 카피 활성화


// 루트 CA 인증서 파일 경로
pub const CA_CERT_FILE: &str = "ssl/ca_cert.pem";
pub const CA_KEY_FILE: &str = "ssl/ca_key.pem";
pub const CA_CERT_CRT_FILE: &str = "ssl/ca_cert.crt";