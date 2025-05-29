/// domain_blocks 테이블 관련 SQL 쿼리

/// 테이블 존재 여부 확인 쿼리
pub const CHECK_TABLE_EXISTS: &str = "
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'domain_blocks'
    )
";

/// 테이블 생성 쿼리
pub const CREATE_TABLE: &str = "
    CREATE TABLE IF NOT EXISTS domain_blocks (
        id SERIAL PRIMARY KEY,
        domain VARCHAR(255) NOT NULL,
        created_by VARCHAR(100) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        description TEXT,
        active BOOLEAN NOT NULL DEFAULT TRUE
    )
";

/// 인덱스 생성 쿼리
pub const CREATE_INDICES: [&str; 2] = [
    "CREATE INDEX IF NOT EXISTS domain_blocks_domain_idx ON domain_blocks(domain)",
    "CREATE INDEX IF NOT EXISTS domain_blocks_active_idx ON domain_blocks(active)"
];

/// 도메인 목록 조회 쿼리
pub const SELECT_ACTIVE_DOMAINS: &str = "
    SELECT domain
    FROM domain_blocks
    WHERE active = TRUE
    ORDER BY domain
";

/// 도메인 추가 쿼리
pub const INSERT_DOMAIN: &str = "
    INSERT INTO domain_blocks (domain, created_by, description)
    VALUES ($1, $2, $3)
    RETURNING id
";

/// 도메인 비활성화 쿼리
pub const DEACTIVATE_DOMAIN: &str = "
    UPDATE domain_blocks
    SET active = FALSE
    WHERE domain = $1
";

/// 도메인 활성화 쿼리
pub const ACTIVATE_DOMAIN: &str = "
    UPDATE domain_blocks
    SET active = TRUE
    WHERE domain = $1
";

/// 도메인 존재 여부 확인 쿼리
pub const CHECK_DOMAIN_EXISTS: &str = "
    SELECT EXISTS (
        SELECT FROM domain_blocks
        WHERE domain = $1
    )
"; 