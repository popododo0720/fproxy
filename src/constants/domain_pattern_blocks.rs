/// domain_pattern_blocks 테이블 관련 SQL 쿼리

/// 테이블 존재 여부 확인 쿼리
pub const CHECK_TABLE_EXISTS: &str = "
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'domain_pattern_blocks'
    )
";

/// 테이블 생성 쿼리
pub const CREATE_TABLE: &str = "
    CREATE TABLE IF NOT EXISTS domain_pattern_blocks (
        id SERIAL PRIMARY KEY,
        pattern VARCHAR(255) NOT NULL,
        created_by VARCHAR(100) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        description TEXT,
        active BOOLEAN NOT NULL DEFAULT TRUE
    )
";

/// 인덱스 생성 쿼리
pub const CREATE_INDICES: [&str; 2] = [
    "CREATE INDEX IF NOT EXISTS domain_pattern_blocks_pattern_idx ON domain_pattern_blocks(pattern)",
    "CREATE INDEX IF NOT EXISTS domain_pattern_blocks_active_idx ON domain_pattern_blocks(active)"
];

/// 패턴 목록 조회 쿼리
pub const SELECT_ACTIVE_PATTERNS: &str = "
    SELECT pattern
    FROM domain_pattern_blocks
    WHERE active = TRUE
    ORDER BY pattern
";