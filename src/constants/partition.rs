// 파티션 관련 SQL 쿼리

/// 파티션 관련 상수 정의

/// 파티션 삭제 쿼리
pub const DROP_PARTITION: &str = "DROP TABLE IF EXISTS $1";

/// 특정 날짜 이전의 파티션 목록 조회 쿼리
pub const LIST_OLD_PARTITIONS: &str = "
    SELECT tablename 
    FROM pg_tables 
    WHERE schemaname = 'public' 
    AND tablename LIKE $1 || '_%'
    AND tablename < $1 || '_' || $2
";

/// 월별 파티션 생성 쿼리
pub const CREATE_MONTHLY_PARTITION: &str = "CREATE TABLE IF NOT EXISTS {} PARTITION OF {} FOR VALUES FROM ('{}') TO ('{}')"; 