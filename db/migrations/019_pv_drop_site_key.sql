BEGIN;
SET LOCAL lock_timeout = '5s';
ALTER TABLE pv_plants DROP COLUMN IF EXISTS site_key;
COMMIT;
