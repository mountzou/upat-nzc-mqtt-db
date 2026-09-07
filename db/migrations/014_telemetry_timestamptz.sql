-- Telemetry instants were historically stored as UTC in timestamp columns.
-- Run only with the matching API/maintenance release and a verified backup.
-- SET LOCAL UTC is essential: PostgreSQL 15 can preserve the heap while
-- changing the type; indexes containing these columns are rebuilt.
\set ON_ERROR_STOP on
BEGIN;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30min';
SELECT pg_advisory_xact_lock(hashtext('telemetry_timestamptz_migration'));

DO $$
DECLARE
    target RECORD;
    current_type REGTYPE;
    old_file OID;
BEGIN
    FOR target IN SELECT * FROM (VALUES
        ('upat_devices', 'created_at'),
        ('shelly_devices', 'created_at'),
        ('upat_raw_messages', 'event_time'),
        ('upat_raw_messages', 'ingestion_time'),
        ('shelly_raw_messages', 'event_time'),
        ('shelly_raw_messages', 'ingestion_time'),
        ('upat_measurements', 'event_time'),
        ('shelly_measurements', 'event_time'),
        ('upat_measurements_5min', 'bucket_start'),
        ('upat_measurements_hourly', 'bucket_start')
    ) AS targets(table_name, column_name)
    LOOP
        SELECT atttypid::regtype INTO STRICT current_type
        FROM pg_attribute
        WHERE attrelid = format('public.%I', target.table_name)::regclass
          AND attname = target.column_name AND NOT attisdropped;
        IF current_type = 'timestamp without time zone'::regtype THEN
            SELECT pg_relation_filenode(format('public.%I', target.table_name)::regclass)
                INTO old_file;
            EXECUTE format('ALTER TABLE public.%I ALTER COLUMN %I TYPE timestamptz USING %I::timestamptz',
                target.table_name, target.column_name, target.column_name);
            IF pg_relation_filenode(format('public.%I', target.table_name)::regclass) <> old_file THEN
                RAISE EXCEPTION 'Unexpected heap rewrite for %; migration rolled back', target.table_name;
            END IF;
        ELSIF current_type <> 'timestamp with time zone'::regtype THEN
            RAISE EXCEPTION 'Unexpected type for %.%: %', target.table_name, target.column_name, current_type;
        END IF;
        EXECUTE format('COMMENT ON COLUMN public.%I.%I IS %L', target.table_name,
            target.column_name, 'Absolute instant. Legacy values interpreted as UTC by migration 014. Application calendar: Europe/Athens.');
    END LOOP;

    -- PV actuals already use timestamptz. Enforce their persisted calendar key
    -- at the database boundary, not independently in each consumer.
    FOR target IN SELECT * FROM (VALUES
        ('pv_plant_readings_5m'), ('pv_device_readings_5m')
    ) AS targets(table_name)
    LOOP
        SELECT atttypid::regtype INTO STRICT current_type
        FROM pg_attribute
        WHERE attrelid = format('public.%I', target.table_name)::regclass
          AND attname = 'observed_at' AND NOT attisdropped;
        IF current_type <> 'timestamp with time zone'::regtype THEN
            RAISE EXCEPTION 'PV observed_at must already be timestamptz';
        END IF;
        IF NOT EXISTS (SELECT 1 FROM pg_constraint
            WHERE conrelid = format('public.%I', target.table_name)::regclass
              AND conname = target.table_name || '_athens_date') THEN
            EXECUTE format('ALTER TABLE public.%I ADD CONSTRAINT %I CHECK (local_date = (observed_at AT TIME ZONE ''Europe/Athens'')::date) NOT VALID',
                target.table_name, target.table_name || '_athens_date');
        END IF;
        EXECUTE format('ALTER TABLE public.%I VALIDATE CONSTRAINT %I',
            target.table_name, target.table_name || '_athens_date');
    END LOOP;

    -- New sessions display timestamptz values with the correct Athens offset.
    -- The stored instants and existing aware PV/energy values do not change.
    EXECUTE format('ALTER DATABASE %I SET timezone TO %L', current_database(), 'Europe/Athens');
END
$$;
-- ALTER TYPE invalidates statistics for the changed columns.
ANALYZE public.upat_devices;
ANALYZE public.shelly_devices;
ANALYZE public.upat_raw_messages;
ANALYZE public.shelly_raw_messages;
ANALYZE public.upat_measurements;
ANALYZE public.shelly_measurements;
ANALYZE public.upat_measurements_5min;
ANALYZE public.upat_measurements_hourly;
COMMIT;
