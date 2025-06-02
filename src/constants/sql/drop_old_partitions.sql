DO $$
DECLARE
    cutoff_date DATE := CURRENT_DATE - ({0} * INTERVAL '1 day');
    cutoff_str TEXT := TO_CHAR(cutoff_date, 'YYYYMMDD');
    table_prefix TEXT := '{1}';
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