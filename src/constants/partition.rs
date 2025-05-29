// 파티션 관련 SQL 쿼리

/// 파티션 존재 여부 확인 쿼리
pub const CHECK_PARTITION_EXISTS: &str = "
    SELECT EXISTS (
        SELECT FROM pg_tables 
        WHERE schemaname = 'public' 
        AND tablename = $1
    )";

/// 테이블 목록 조회 쿼리
pub const LIST_TABLES_QUERY: &str = "
    SELECT tablename 
    FROM pg_tables 
    WHERE schemaname = 'public' AND tablename LIKE $1
"; 