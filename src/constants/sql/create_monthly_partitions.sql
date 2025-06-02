DO $$
DECLARE
    current_month DATE := DATE_TRUNC('month', CURRENT_DATE);
    future_month DATE;
    partition_name TEXT;
    table_prefix TEXT := '{0}';
    months_ahead INTEGER := {1};
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