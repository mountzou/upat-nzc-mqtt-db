BEGIN;
SET LOCAL lock_timeout = '5s';
ALTER TABLE pv_ingestion_runs DROP COLUMN IF EXISTS source_kind;
ALTER TABLE pv_device_readings_5m DROP COLUMN IF EXISTS source_kind;
ALTER TABLE pv_plant_readings_5m DROP COLUMN IF EXISTS source_kind;
COMMIT;
