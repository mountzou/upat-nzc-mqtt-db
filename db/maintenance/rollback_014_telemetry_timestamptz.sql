-- Restore the legacy UTC-naive contract. Use only with the previous code.
-- No measurements are removed; the inverse type cast preserves instants.
\set ON_ERROR_STOP on
BEGIN;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30min';
SELECT pg_advisory_xact_lock(hashtext('telemetry_timestamptz_migration'));
DO $$
DECLARE target RECORD; current_type REGTYPE;
BEGIN
    FOR target IN SELECT * FROM (VALUES
        ('upat_devices', 'created_at'), ('shelly_devices', 'created_at'),
        ('upat_raw_messages', 'event_time'), ('upat_raw_messages', 'ingestion_time'),
        ('shelly_raw_messages', 'event_time'), ('shelly_raw_messages', 'ingestion_time'),
        ('upat_measurements', 'event_time'), ('shelly_measurements', 'event_time'),
        ('upat_measurements_5min', 'bucket_start'), ('upat_measurements_hourly', 'bucket_start')
    ) AS targets(table_name, column_name)
    LOOP
        SELECT atttypid::regtype INTO STRICT current_type FROM pg_attribute
        WHERE attrelid = format('public.%I', target.table_name)::regclass
          AND attname = target.column_name AND NOT attisdropped;
        IF current_type = 'timestamp with time zone'::regtype THEN
            EXECUTE format('ALTER TABLE public.%I ALTER COLUMN %I TYPE timestamp USING %I::timestamp',
                target.table_name, target.column_name, target.column_name);
        ELSIF current_type <> 'timestamp without time zone'::regtype THEN
            RAISE EXCEPTION 'Unexpected type for %.%', target.table_name, target.column_name;
        END IF;
        EXECUTE format('COMMENT ON COLUMN public.%I.%I IS NULL', target.table_name, target.column_name);
    END LOOP;
    ALTER TABLE public.pv_plant_readings_5m DROP CONSTRAINT IF EXISTS pv_plant_readings_5m_athens_date;
    ALTER TABLE public.pv_device_readings_5m DROP CONSTRAINT IF EXISTS pv_device_readings_5m_athens_date;
    EXECUTE format('ALTER DATABASE %I RESET timezone', current_database());
END
$$;
ANALYZE public.upat_devices;
ANALYZE public.shelly_devices;
ANALYZE public.upat_raw_messages;
ANALYZE public.shelly_raw_messages;
ANALYZE public.upat_measurements;
ANALYZE public.shelly_measurements;
ANALYZE public.upat_measurements_5min;
ANALYZE public.upat_measurements_hourly;
COMMIT;
