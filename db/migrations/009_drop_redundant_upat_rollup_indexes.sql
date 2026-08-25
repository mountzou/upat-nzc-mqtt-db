-- Apply with psql in autocommit mode; DROP INDEX CONCURRENTLY cannot run
-- inside an explicit transaction block.
DROP INDEX CONCURRENTLY IF EXISTS idx_upat_measurements_5min_device_metric_time;
DROP INDEX CONCURRENTLY IF EXISTS idx_upat_measurements_hourly_device_metric_time;
