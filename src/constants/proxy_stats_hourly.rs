/// 시간당 프록시 통계 관련 SQL 쿼리

/// 테이블 존재 여부 확인 쿼리
pub const CHECK_TABLE_EXISTS: &str = "
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'proxy_stats_hourly'
    )
";

/// 테이블 생성 쿼리
pub const CREATE_TABLE: &str = "
    CREATE TABLE IF NOT EXISTS proxy_stats_hourly (
        id SERIAL,
        timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        http_connections_avg DOUBLE PRECISION NOT NULL,
        http_bytes_in DOUBLE PRECISION NOT NULL,
        http_bytes_out DOUBLE PRECISION NOT NULL,
        tls_connections_avg DOUBLE PRECISION NOT NULL,
        tls_bytes_in DOUBLE PRECISION NOT NULL,
        tls_bytes_out DOUBLE PRECISION NOT NULL,
        uptime_seconds BIGINT NOT NULL,
        PRIMARY KEY (id, timestamp)
    ) PARTITION BY RANGE (timestamp)";

/// 기본 인덱스 생성 쿼리 - 부모 테이블에만 적용
pub const CREATE_INDICES: [&str; 1] = [
    "CREATE INDEX IF NOT EXISTS proxy_stats_hourly_timestamp_idx ON proxy_stats_hourly(timestamp)"
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
    INSERT INTO proxy_stats_hourly (
        timestamp, 
        http_connections_avg, 
        http_bytes_in, 
        http_bytes_out, 
        tls_connections_avg, 
        tls_bytes_in, 
        tls_bytes_out, 
        uptime_seconds
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8
    )
"; 