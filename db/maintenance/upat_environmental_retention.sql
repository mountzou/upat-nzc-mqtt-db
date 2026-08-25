-- Roll up old UPAT environmental measurements, then trim high-resolution rows.
--
-- Usage example:
--   docker exec -i iot_postgres psql -U postgres -d iot_db \
--     -v high_res_retention="'7 days'" \
--     -v raw_retention="'14 days'" \
--     < db/maintenance/upat_environmental_retention.sql
--
-- The rollup INSERT statements are idempotent. Run them before deleting old
-- high-resolution rows so historical API/reporting data can be rebuilt.

\set ON_ERROR_STOP on

\if :{?high_res_retention}
\else
\set high_res_retention '''7 days'''
\endif

\if :{?raw_retention}
\else
\set raw_retention '''14 days'''
\endif

BEGIN;

CREATE TABLE IF NOT EXISTS upat_measurements_5min (
    device_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    unit TEXT,
    bucket_start TIMESTAMP NOT NULL,
    value_avg DOUBLE PRECISION,
    value_min DOUBLE PRECISION,
    value_max DOUBLE PRECISION,
    sample_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (device_id, metric, bucket_start)
);

CREATE TABLE IF NOT EXISTS upat_measurements_hourly (
    device_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    unit TEXT,
    bucket_start TIMESTAMP NOT NULL,
    value_avg DOUBLE PRECISION,
    value_min DOUBLE PRECISION,
    value_max DOUBLE PRECISION,
    sample_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (device_id, metric, bucket_start)
);

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
    date_bin(INTERVAL '5 minutes', event_time, TIMESTAMP '2001-01-01 00:00:00') AS bucket_start,
    AVG(value) AS value_avg,
    MIN(value) AS value_min,
    MAX(value) AS value_max,
    COUNT(*)::INTEGER AS sample_count,
    NOW() AS updated_at
FROM upat_measurements
WHERE event_time IS NOT NULL
GROUP BY device_id, metric, unit, bucket_start
ON CONFLICT (device_id, metric, bucket_start)
DO UPDATE SET
    unit = EXCLUDED.unit,
    value_avg = EXCLUDED.value_avg,
    value_min = EXCLUDED.value_min,
    value_max = EXCLUDED.value_max,
    sample_count = EXCLUDED.sample_count,
    updated_at = NOW();

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
    date_bin(INTERVAL '1 hour', event_time, TIMESTAMP '2001-01-01 00:00:00') AS bucket_start,
    AVG(value) AS value_avg,
    MIN(value) AS value_min,
    MAX(value) AS value_max,
    COUNT(*)::INTEGER AS sample_count,
    NOW() AS updated_at
FROM upat_measurements
WHERE event_time IS NOT NULL
GROUP BY device_id, metric, unit, bucket_start
ON CONFLICT (device_id, metric, bucket_start)
DO UPDATE SET
    unit = EXCLUDED.unit,
    value_avg = EXCLUDED.value_avg,
    value_min = EXCLUDED.value_min,
    value_max = EXCLUDED.value_max,
    sample_count = EXCLUDED.sample_count,
    updated_at = NOW();

DELETE FROM upat_measurements
WHERE event_time IS NOT NULL
  AND event_time < NOW() - (:high_res_retention)::INTERVAL;

DELETE FROM upat_raw_messages
WHERE event_time IS NOT NULL
  AND event_time < NOW() - (:raw_retention)::INTERVAL;

COMMIT;

VACUUM (ANALYZE) upat_measurements;
VACUUM (ANALYZE) upat_raw_messages;
VACUUM (ANALYZE) upat_measurements_5min;
VACUUM (ANALYZE) upat_measurements_hourly;
