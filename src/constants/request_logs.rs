/// request_logs 테이블 관련 SQL 쿼리

/// 테이블 생성 쿼리
pub const CREATE_TABLE: &str = "
    CREATE TABLE IF NOT EXISTS request_logs (
        id BIGSERIAL,
        host TEXT NOT NULL,
        method TEXT NOT NULL,
        path TEXT NOT NULL,
        header TEXT NOT NULL,
        body TEXT,
        timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        session_id TEXT NOT NULL,
        client_ip TEXT NOT NULL,
        target_ip TEXT NOT NULL,
        response_time BIGINT,
        is_rejected BOOLEAN NOT NULL DEFAULT FALSE,
        is_tls BOOLEAN NOT NULL DEFAULT FALSE,
        PRIMARY KEY (id, timestamp)
    ) PARTITION BY RANGE (timestamp)";

/// 기본 인덱스 생성 쿼리 - 부모 테이블에만 적용
pub const CREATE_INDICES: [&str; 4] = [
    "CREATE INDEX IF NOT EXISTS request_logs_host_idx ON request_logs(host)",
    "CREATE INDEX IF NOT EXISTS request_logs_timestamp_idx ON request_logs(timestamp)",
    "CREATE INDEX IF NOT EXISTS request_logs_is_rejected_idx ON request_logs(is_rejected)",
    "CREATE INDEX IF NOT EXISTS request_logs_is_tls_idx ON request_logs(is_tls)"
];

/// 파티션별 인덱스 생성 쿼리 - 각 파티션에 개별 적용
pub fn create_partition_indices(partition_name: &str) -> Vec<String> {
    vec![
        // 기본 인덱스 - 파티션별로 필요한 인덱스
        format!(
            "CREATE INDEX IF NOT EXISTS {}_timestamp_idx ON {} (timestamp)",
            partition_name, partition_name
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {}_host_idx ON {} (host)",
            partition_name, partition_name
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {}_is_rejected_idx ON {} (is_rejected)",
            partition_name, partition_name
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {}_is_tls_idx ON {} (is_tls)",
            partition_name, partition_name
        ),
        // 추가 인덱스
        format!(
            "CREATE INDEX IF NOT EXISTS {}_client_ip_idx ON {} (client_ip)",
            partition_name, partition_name
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {}_target_ip_idx ON {} (target_ip)",
            partition_name, partition_name
        )
    ]
}