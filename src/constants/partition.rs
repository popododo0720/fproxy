// 파티션 관련 SQL 쿼리

/// 현재 및 미래 파티션 자동 생성 스크립트
pub const CREATE_FUTURE_PARTITIONS: &str = "
DO $$
DECLARE
    current_date DATE := CURRENT_DATE;
    future_date DATE;
    partition_name TEXT;
    table_prefix TEXT := $1;
    days_ahead INTEGER := $2;
BEGIN
    FOR i IN 0..days_ahead LOOP
        future_date := current_date + (i * INTERVAL '1 day');
        partition_name := table_prefix || '_' || TO_CHAR(future_date, 'YYYYMMDD');
        
        EXECUTE 'CREATE TABLE IF NOT EXISTS ' || partition_name || 
                ' PARTITION OF ' || table_prefix || 
                ' FOR VALUES FROM (''' || future_date || 
                ''') TO (''' || (future_date + INTERVAL '1 day') || ''')';
        
        RAISE NOTICE 'Created partition: %', partition_name;
    END LOOP;
END $$;
";

/// 월간 파티션 자동 생성 스크립트
pub const CREATE_MONTHLY_PARTITIONS: &str = "
DO $$
DECLARE
    current_month DATE := DATE_TRUNC('month', CURRENT_DATE);
    future_month DATE;
    partition_name TEXT;
    table_prefix TEXT := $1;
    months_ahead INTEGER := $2;
BEGIN
    FOR i IN 0..months_ahead LOOP
        future_month := current_month + (i * INTERVAL '1 month');
        partition_name := table_prefix || '_' || TO_CHAR(future_month, 'YYYYMM');
        
        EXECUTE 'CREATE TABLE IF NOT EXISTS ' || partition_name || 
                ' PARTITION OF ' || table_prefix || 
                ' FOR VALUES FROM (''' || future_month || 
                ''') TO (''' || (future_month + INTERVAL '1 month') || ''')';
        
        RAISE NOTICE 'Created monthly partition: %', partition_name;
    END LOOP;
END $$;
";

/// 오래된 파티션 정리 스크립트
pub const DROP_OLD_PARTITIONS: &str = "
DO $$
DECLARE
    cutoff_date DATE := CURRENT_DATE - ($2 * INTERVAL '1 day');
    cutoff_str TEXT := TO_CHAR(cutoff_date, 'YYYYMMDD');
    table_prefix TEXT := $1;
    rec RECORD;
BEGIN
    FOR rec IN SELECT tablename 
               FROM pg_tables 
               WHERE schemaname = 'public' 
               AND tablename LIKE table_prefix || '_%'
               AND tablename < CONCAT(table_prefix, '_', cutoff_str)
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || rec.tablename;
        RAISE NOTICE 'Dropped old partition: %', rec.tablename;
    END LOOP;
END $$;
"; 