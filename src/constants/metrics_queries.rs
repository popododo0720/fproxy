/// metrics 모듈에서 사용하는 SQL 쿼리 모음

/// proxy_stats 테이블 존재 여부 확인 쿼리
pub const CHECK_PROXY_STATS_TABLE_EXISTS: &str = "
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'proxy_stats'
    )
";

/// proxy_stats_hourly 테이블 존재 여부 확인 쿼리
pub const CHECK_PROXY_STATS_HOURLY_TABLE_EXISTS: &str = "
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'proxy_stats_hourly'
    )
";

/// proxy_stats 테이블 생성 쿼리
pub const CREATE_PROXY_STATS_TABLE: &str = "
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
    ) PARTITION BY RANGE (timestamp)
";

/// proxy_stats_hourly 테이블 생성 쿼리
pub const CREATE_PROXY_STATS_HOURLY_TABLE: &str = "
    CREATE TABLE IF NOT EXISTS proxy_stats_hourly (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        http_connections_avg DOUBLE PRECISION NOT NULL,
        http_bytes_in DOUBLE PRECISION NOT NULL,
        http_bytes_out DOUBLE PRECISION NOT NULL,
        tls_connections_avg DOUBLE PRECISION NOT NULL,
        tls_bytes_in DOUBLE PRECISION NOT NULL,
        tls_bytes_out DOUBLE PRECISION NOT NULL,
        uptime_seconds BIGINT NOT NULL
    )
";

/// proxy_stats 테이블 인덱스 생성 쿼리
pub const CREATE_PROXY_STATS_INDEX: &str = "
    CREATE INDEX IF NOT EXISTS proxy_stats_timestamp_idx ON proxy_stats(timestamp)
";

/// proxy_stats_hourly 테이블 인덱스 생성 쿼리
pub const CREATE_PROXY_STATS_HOURLY_INDEX: &str = "
    CREATE INDEX IF NOT EXISTS proxy_stats_hourly_timestamp_idx ON proxy_stats_hourly(timestamp)
";

/// 파티션 존재 여부 확인 쿼리
pub const CHECK_PARTITION_EXISTS: &str = "
    SELECT EXISTS (
        SELECT FROM pg_tables 
        WHERE schemaname = 'public' 
        AND tablename = $1
    )
";

/// proxy_stats 테이블에 통계 데이터 삽입 쿼리
pub const INSERT_PROXY_STATS: &str = "
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

/// proxy_stats_hourly 테이블에 통계 데이터 삽입 쿼리
pub const INSERT_PROXY_STATS_HOURLY: &str = "
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