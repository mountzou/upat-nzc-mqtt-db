-- Recompute recent UPAT rollups and atomically initialize their watermark.
--
-- A SHARE lock briefly blocks new measurement inserts while reads continue.
-- This closes the race between the final rollup refresh and MAX(id).
--
-- Usage:
--   docker exec -i iot_postgres psql -U postgres -d iot_db \
--     -v rebuild_lookback="'2 days'" \
--     < db/maintenance/upat_initialize_rollup_state.sql

\set ON_ERROR_STOP on

\if :{?rebuild_lookback}
\else
\set rebuild_lookback '''2 days'''
\endif

BEGIN;

SELECT pg_advisory_xact_lock(hashtext('upat_incremental_rollups'));

LOCK TABLE upat_measurements IN SHARE MODE;

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
    FROM upat_measurements
    WHERE event_time >= (
        NOW() AT TIME ZONE 'UTC'
        - (:rebuild_lookback)::INTERVAL
    )
      AND event_time IS NOT NULL
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
    unit = EXCLUDED.unit,
    value_avg = EXCLUDED.value_avg,
    value_min = EXCLUDED.value_min,
    value_max = EXCLUDED.value_max,
    sample_count = EXCLUDED.sample_count,
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
    FROM upat_measurements
    WHERE event_time >= (
        NOW() AT TIME ZONE 'UTC'
        - (:rebuild_lookback)::INTERVAL
    )
      AND event_time IS NOT NULL
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
    unit = EXCLUDED.unit,
    value_avg = EXCLUDED.value_avg,
    value_min = EXCLUDED.value_min,
    value_max = EXCLUDED.value_max,
    sample_count = EXCLUDED.sample_count,
    updated_at = NOW();

INSERT INTO upat_rollup_state (
    pipeline_name,
    last_measurement_id,
    updated_at
)
SELECT
    'upat',
    COALESCE(MAX(id), 0),
    NOW()
FROM upat_measurements
ON CONFLICT (pipeline_name)
DO UPDATE SET
    last_measurement_id = EXCLUDED.last_measurement_id,
    updated_at = NOW();

SELECT
    state.last_measurement_id,
    source.max_measurement_id,
    source.max_measurement_id - state.last_measurement_id AS id_lag,
    state.updated_at
FROM upat_rollup_state AS state
CROSS JOIN (
    SELECT COALESCE(MAX(id), 0) AS max_measurement_id
    FROM upat_measurements
) AS source
WHERE state.pipeline_name = 'upat';

COMMIT;
