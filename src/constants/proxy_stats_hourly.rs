/// proxy_stats_hourly 테이블 관련 SQL 쿼리

/// 테이블 생성 쿼리
pub const CREATE_TABLE: &str = "
    CREATE TABLE IF NOT EXISTS proxy_stats_hourly (
        id SERIAL,
        host TEXT NOT NULL,
        path TEXT NOT NULL,
        hour TIMESTAMPTZ NOT NULL,
        request_count INT NOT NULL DEFAULT 0,
        avg_response_time BIGINT,
        min_response_time BIGINT,
        max_response_time BIGINT,
        error_count INT NOT NULL DEFAULT 0,
        timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
    ) PARTITION BY RANGE (timestamp)";

/// 기본 인덱스 생성 쿼리
pub const CREATE_INDICES: [&str; 3] = [
    "CREATE INDEX IF NOT EXISTS proxy_stats_hourly_host_idx ON proxy_stats_hourly(host)",
    "CREATE INDEX IF NOT EXISTS proxy_stats_hourly_timestamp_idx ON proxy_stats_hourly(timestamp)",
    "CREATE INDEX IF NOT EXISTS proxy_stats_hourly_hour_idx ON proxy_stats_hourly(hour)"
];

/// 파티션별 추가 인덱스 생성 쿼리
pub fn create_partition_indices(partition_name: &str) -> Vec<String> {
    vec![
        // 기본 인덱스
        format!(
            "CREATE INDEX IF NOT EXISTS {}_hour_idx ON {} (hour)",
            partition_name, partition_name
        ),
        // 특수 인덱스
        format!(
            "CREATE INDEX IF NOT EXISTS {}_request_count_idx ON {} (request_count)",
            partition_name, partition_name
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {}_error_count_idx ON {} (error_count)",
            partition_name, partition_name
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {}_avg_response_time_idx ON {} (avg_response_time)",
            partition_name, partition_name
        )
    ]
} 