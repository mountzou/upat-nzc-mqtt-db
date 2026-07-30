-- Delete one bounded batch of high-resolution UPAT measurements.
--
-- Rows are eligible only after the incremental rollup job has processed their
-- measurement IDs. This prevents retention from deleting unaggregated data.
--
-- Usage:
--   docker exec -i iot_postgres psql -U postgres -d iot_db \
--     -v high_res_retention="'7 days'" \
--     -v delete_batch_size=500000 \
--     < db/maintenance/upat_retention_batch.sql

\set ON_ERROR_STOP on

\if :{?high_res_retention}
\else
\set high_res_retention '''7 days'''
\endif

\if :{?delete_batch_size}
\else
\set delete_batch_size 500000
\endif

BEGIN;

SELECT pg_advisory_xact_lock(hashtext('upat_retention_batch'));

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM upat_rollup_state
        WHERE pipeline_name = 'upat'
    ) THEN
        RAISE EXCEPTION
            'UPAT rollup state is not initialized; refusing retention';
    END IF;
END
$$;

WITH doomed AS (
    SELECT id
    FROM upat_measurements
    WHERE event_time IS NOT NULL
      AND event_time < NOW() - (:high_res_retention)::INTERVAL
      AND id <= (
          SELECT last_measurement_id
          FROM upat_rollup_state
          WHERE pipeline_name = 'upat'
      )
    ORDER BY id
    LIMIT :delete_batch_size
)
DELETE FROM upat_measurements
WHERE id IN (SELECT id FROM doomed);

COMMIT;
