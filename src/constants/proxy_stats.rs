/// proxy_stats 테이블 관련 SQL 쿼리

/// 테이블 존재 여부 확인 쿼리
pub const CHECK_TABLE_EXISTS: &str = "
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'proxy_stats'
    )
";

/// 테이블 생성 쿼리
pub const CREATE_TABLE: &str = "
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
        seconds_since_reset BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (id, timestamp)
    ) PARTITION BY RANGE (timestamp)";

/// 기본 인덱스 생성 쿼리 - 부모 테이블에만 적용
pub const CREATE_INDICES: [&str; 1] = [
    "CREATE INDEX IF NOT EXISTS proxy_stats_timestamp_idx ON proxy_stats(timestamp)"
];

/// 파티션별 인덱스 생성 쿼리 - 각 파티션에 개별 적용
pub fn create_partition_indices(partition_name: &str) -> Vec<String> {
    vec![
        // 기본 인덱스 - 파티션별로 필요한 인덱스
        format!(
            "CREATE INDEX IF NOT EXISTS {}_timestamp_idx ON {} (timestamp)",
            partition_name, partition_name
        )
    ]
}

/// 파티션 존재 여부 확인 쿼리
pub const CHECK_PARTITION_EXISTS: &str = "
    SELECT EXISTS (
        SELECT FROM pg_tables 
        WHERE schemaname = 'public' 
        AND tablename = $1
    )
";

/// 통계 데이터 삽입 쿼리
pub const INSERT_STATS: &str = "
    INSERT INTO proxy_stats (
        timestamp, 
        http_active_connections, 
        http_bytes_in, 
        http_bytes_out, 
        tls_active_connections, 
        tls_bytes_in, 
        tls_bytes_out, 
        uptime_seconds,
        seconds_since_reset
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9
    )
";

/// 마지막 메트릭스 값 조회 쿼리
pub const SELECT_LAST_METRICS: &str = "
    SELECT 
        http_active_connections,
        http_bytes_in,
        http_bytes_out,
        tls_active_connections,
        tls_bytes_in,
        tls_bytes_out
    FROM proxy_stats
    ORDER BY timestamp DESC
    LIMIT 1
";

/// 파티션 생성 쿼리 포맷 문자열
pub const CREATE_PARTITION_FORMAT: &str = "
    CREATE TABLE IF NOT EXISTS {} PARTITION OF proxy_stats 
    FOR VALUES FROM ('{}') TO ('{}')
";

/// 파티션 인덱스 생성 쿼리 포맷 문자열
pub const CREATE_PARTITION_INDEX_FORMAT: &str = "
    CREATE INDEX IF NOT EXISTS {}_timestamp_idx ON {} (timestamp)
";