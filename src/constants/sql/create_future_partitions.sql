DO $$
DECLARE
    current_date DATE := CURRENT_DATE;
    future_date DATE;
    partition_name TEXT;
    table_prefix TEXT := '{0}';
    days_ahead INTEGER := {1};
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