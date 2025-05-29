/// proxy_stats 테이블 관련 SQL 쿼리

/// 테이블 생성 쿼리
pub const CREATE_TABLE: &str = "
    CREATE TABLE IF NOT EXISTS proxy_stats (
        id SERIAL,
        host TEXT NOT NULL,
        path TEXT NOT NULL,
        status_code INT,
        response_time BIGINT,
        timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        target_ip TEXT NOT NULL,
        is_tls BOOLEAN NOT NULL DEFAULT FALSE
    ) PARTITION BY RANGE (timestamp)";

/// 기본 인덱스 생성 쿼리
pub const CREATE_INDICES: [&str; 3] = [
    "CREATE INDEX IF NOT EXISTS proxy_stats_host_idx ON proxy_stats(host)",
    "CREATE INDEX IF NOT EXISTS proxy_stats_timestamp_idx ON proxy_stats(timestamp)",
    "CREATE INDEX IF NOT EXISTS proxy_stats_status_code_idx ON proxy_stats(status_code)"
];

/// 파티션별 추가 인덱스 생성 쿼리
pub fn create_partition_indices(partition_name: &str) -> Vec<String> {
    vec![
        // 기본 인덱스
        format!(
            "CREATE INDEX IF NOT EXISTS {}_status_code_idx ON {} (status_code)",
            partition_name, partition_name
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {}_is_tls_idx ON {} (is_tls)",
            partition_name, partition_name
        ),
        // 특수 인덱스
        format!(
            "CREATE INDEX IF NOT EXISTS {}_target_ip_idx ON {} (target_ip)",
            partition_name, partition_name
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {}_response_time_idx ON {} (response_time)",
            partition_name, partition_name
        )
    ]
}