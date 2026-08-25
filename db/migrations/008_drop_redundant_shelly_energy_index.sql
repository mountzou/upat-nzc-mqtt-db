-- Apply with psql in autocommit mode; DROP INDEX CONCURRENTLY cannot run
-- inside an explicit transaction block.
DROP INDEX CONCURRENTLY IF EXISTS idx_shelly_energy_device_time;
