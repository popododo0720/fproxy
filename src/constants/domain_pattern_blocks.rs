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

/// 패턴 추가 쿼리
pub const INSERT_PATTERN: &str = "
    INSERT INTO domain_pattern_blocks (pattern, created_by, description)
    VALUES ($1, $2, $3)
    RETURNING id
";

/// 패턴 비활성화 쿼리
pub const DEACTIVATE_PATTERN: &str = "
    UPDATE domain_pattern_blocks
    SET active = FALSE
    WHERE pattern = $1
";

/// 패턴 활성화 쿼리
pub const ACTIVATE_PATTERN: &str = "
    UPDATE domain_pattern_blocks
    SET active = TRUE
    WHERE pattern = $1
";

/// 패턴 존재 여부 확인 쿼리
pub const CHECK_PATTERN_EXISTS: &str = "
    SELECT EXISTS (
        SELECT FROM domain_pattern_blocks
        WHERE pattern = $1
    )
"; 