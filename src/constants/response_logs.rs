/// response_logs 테이블 관련 SQL 쿼리

/// 테이블 생성 쿼리
pub const CREATE_TABLE: &str = "
    CREATE TABLE IF NOT EXISTS response_logs (
        id BIGSERIAL,
        session_id TEXT NOT NULL,
        status_code INTEGER NOT NULL,
        response_time BIGINT NOT NULL,
        response_size BIGINT NOT NULL,
        timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        headers TEXT NOT NULL,
        body_preview TEXT,
        PRIMARY KEY (id, timestamp)
    ) PARTITION BY RANGE (timestamp)";

/// 로그 삽입 쿼리
pub const INSERT_LOG: &str = "
    INSERT INTO response_logs (
        session_id, status_code, response_time, response_size, 
        timestamp, headers, body_preview
    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
";

/// 복사 모드를 위한 쿼리
pub const COPY_LOGS: &str = "
    COPY response_logs (
        session_id, status_code, response_time, response_size, 
        timestamp, headers, body_preview
    ) FROM STDIN BINARY
";

/// 응답 시간 업데이트 쿼리
pub const UPDATE_RESPONSE_TIME: &str = "
    UPDATE response_logs SET response_time = $2 
    WHERE session_id = $1 AND response_time = 0
";

/// 기본 인덱스 생성 쿼리 - 부모 테이블에만 적용
pub const CREATE_INDICES: [&str; 3] = [
    "CREATE INDEX IF NOT EXISTS response_logs_session_id_idx ON response_logs(session_id)",
    "CREATE INDEX IF NOT EXISTS response_logs_timestamp_idx ON response_logs(timestamp)",
    "CREATE INDEX IF NOT EXISTS response_logs_status_code_idx ON response_logs(status_code)"
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
            "CREATE INDEX IF NOT EXISTS {}_session_id_idx ON {} (session_id)",
            partition_name, partition_name
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {}_status_code_idx ON {} (status_code)",
            partition_name, partition_name
        )
    ]
}

/// 테이블 존재 여부 확인 쿼리
pub const CHECK_TABLE_EXISTS: &str = "
    SELECT EXISTS (
        SELECT FROM pg_tables
        WHERE schemaname = 'public'
        AND tablename = 'response_logs'
    )
";

/// 파티션 테이블 생성 쿼리
pub fn create_partition_table(partition_name: &str, start_date: &str, end_date: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {} PARTITION OF response_logs 
         FOR VALUES FROM ('{}') TO ('{}');",
        partition_name, start_date, end_date
    )
}

/// 파티션 테이블 삭제 쿼리
pub fn drop_partition_table(partition_name: &str) -> String {
    format!("DROP TABLE IF EXISTS {}", partition_name)
} 