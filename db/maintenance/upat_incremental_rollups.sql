-- Incrementally roll up UPAT measurements that have not been processed yet.
--
-- The state row must be initialized after a complete rollup backfill. Keeping
-- the state update in the same transaction as both upserts makes retries safe.
--
-- Usage:
--   docker exec -i iot_postgres psql -U postgres -d iot_db \
--     -v batch_size=500000 \
--     < db/maintenance/upat_incremental_rollups.sql

\set ON_ERROR_STOP on

\if :{?batch_size}
\else
\set batch_size 500000
\endif

BEGIN;

SELECT pg_advisory_xact_lock(hashtext('upat_incremental_rollups'));

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM upat_rollup_state
        WHERE pipeline_name = 'upat'
    ) THEN
        RAISE EXCEPTION
            'UPAT rollup state is not initialized; backfill and seed it first';
    END IF;
END
$$;

SELECT last_measurement_id
FROM upat_rollup_state
WHERE pipeline_name = 'upat'
FOR UPDATE;

CREATE TEMP TABLE upat_rollup_batch
ON COMMIT DROP
AS
SELECT
    id,
    device_id,
    metric,
    value,
    unit,
    event_time
FROM upat_measurements
WHERE id > (
    SELECT last_measurement_id
    FROM upat_rollup_state
    WHERE pipeline_name = 'upat'
)
ORDER BY id
LIMIT :batch_size;

WITH aggregated AS (
    SELECT
        device_id,
        metric,
        MAX(unit) AS unit,
        date_bin(
            INTERVAL '5 minutes',
            event_time,
            TIMESTAMP '2001-01-01 00:00:00'
        ) AS bucket_start,
        AVG(value)::DOUBLE PRECISION AS value_avg,
        MIN(value)::DOUBLE PRECISION AS value_min,
        MAX(value)::DOUBLE PRECISION AS value_max,
        COUNT(*)::INTEGER AS sample_count
    FROM upat_rollup_batch
    WHERE event_time IS NOT NULL
      AND value IS NOT NULL
    GROUP BY
        device_id,
        metric,
        date_bin(
            INTERVAL '5 minutes',
            event_time,
            TIMESTAMP '2001-01-01 00:00:00'
        )
)
INSERT INTO upat_measurements_5min (
    device_id,
    metric,
    unit,
    bucket_start,
    value_avg,
    value_min,
    value_max,
    sample_count,
    updated_at
)
SELECT
    device_id,
    metric,
    unit,
    bucket_start,
    value_avg,
    value_min,
    value_max,
    sample_count,
    NOW()
FROM aggregated
ON CONFLICT (device_id, metric, bucket_start)
DO UPDATE SET
    unit = COALESCE(EXCLUDED.unit, upat_measurements_5min.unit),
    value_avg = (
        upat_measurements_5min.value_avg
            * upat_measurements_5min.sample_count
        + EXCLUDED.value_avg * EXCLUDED.sample_count
    ) / NULLIF(
        upat_measurements_5min.sample_count + EXCLUDED.sample_count,
        0
    ),
    value_min = LEAST(
        upat_measurements_5min.value_min,
        EXCLUDED.value_min
    ),
    value_max = GREATEST(
        upat_measurements_5min.value_max,
        EXCLUDED.value_max
    ),
    sample_count = (
        upat_measurements_5min.sample_count + EXCLUDED.sample_count
    ),
    updated_at = NOW();

WITH aggregated AS (
    SELECT
        device_id,
        metric,
        MAX(unit) AS unit,
        date_bin(
            INTERVAL '1 hour',
            event_time,
            TIMESTAMP '2001-01-01 00:00:00'
        ) AS bucket_start,
        AVG(value)::DOUBLE PRECISION AS value_avg,
        MIN(value)::DOUBLE PRECISION AS value_min,
        MAX(value)::DOUBLE PRECISION AS value_max,
        COUNT(*)::INTEGER AS sample_count
    FROM upat_rollup_batch
    WHERE event_time IS NOT NULL
      AND value IS NOT NULL
    GROUP BY
        device_id,
        metric,
        date_bin(
            INTERVAL '1 hour',
            event_time,
            TIMESTAMP '2001-01-01 00:00:00'
        )
)
INSERT INTO upat_measurements_hourly (
    device_id,
    metric,
    unit,
    bucket_start,
    value_avg,
    value_min,
    value_max,
    sample_count,
    updated_at
)
SELECT
    device_id,
    metric,
    unit,
    bucket_start,
    value_avg,
    value_min,
    value_max,
    sample_count,
    NOW()
FROM aggregated
ON CONFLICT (device_id, metric, bucket_start)
DO UPDATE SET
    unit = COALESCE(EXCLUDED.unit, upat_measurements_hourly.unit),
    value_avg = (
        upat_measurements_hourly.value_avg
            * upat_measurements_hourly.sample_count
        + EXCLUDED.value_avg * EXCLUDED.sample_count
    ) / NULLIF(
        upat_measurements_hourly.sample_count + EXCLUDED.sample_count,
        0
    ),
    value_min = LEAST(
        upat_measurements_hourly.value_min,
        EXCLUDED.value_min
    ),
    value_max = GREATEST(
        upat_measurements_hourly.value_max,
        EXCLUDED.value_max
    ),
    sample_count = (
        upat_measurements_hourly.sample_count + EXCLUDED.sample_count
    ),
    updated_at = NOW();

UPDATE upat_rollup_state
SET
    last_measurement_id = COALESCE(
        (SELECT MAX(id) FROM upat_rollup_batch),
        last_measurement_id
    ),
    updated_at = NOW()
WHERE pipeline_name = 'upat';

SELECT
    COUNT(*) AS processed_rows,
    MIN(id) AS first_processed_id,
    MAX(id) AS last_processed_id
FROM upat_rollup_batch;

COMMIT;
