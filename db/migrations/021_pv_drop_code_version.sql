BEGIN;
SET LOCAL lock_timeout = '5s';
ALTER TABLE pv_ingestion_runs DROP COLUMN IF EXISTS code_version;
COMMIT;
