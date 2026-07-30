# UPAT rollup and retention jobs

`upat_incremental_rollups.sql` processes new `upat_measurements` rows in
primary-key order. A watermark in `upat_rollup_state` records the last
measurement included in both rollup tables. The rollup upserts and watermark
advance commit in one transaction, so a failed run can be retried safely.

`upat_retention_batch.sql` deletes one bounded batch of measurements older than
the configured retention period. It refuses to run unless the watermark exists
and never deletes an ID newer than the watermark.

## First production activation

1. Apply `db/migrations/006_upat_rollup_state.sql`.
2. Stop the TTN ingestor briefly.
3. Recompute all still-open 5-minute and hourly buckets from raw measurements.
4. Seed `upat_rollup_state.last_measurement_id` with the current maximum
   `upat_measurements.id`.
5. Run `upat_incremental_rollups.sql` once and confirm `processed_rows = 0`.
6. Restart the TTN ingestor.
7. Install the cron entries below.

Do not seed the watermark before the existing rollups are fully caught up.
Doing so would mark measurements as processed without preserving their
aggregates.

## Example VPS cron

Run incremental rollups every five minutes:

```cron
*/5 * * * * flock -n /tmp/upat-rollups.lock sh -c 'cd /opt/upat-nzc-mqtt-db && docker exec -i iot_postgres psql -U postgres -d iot_db -v batch_size=500000 < db/maintenance/upat_incremental_rollups.sql' >> /var/log/upat_rollups.log 2>&1
```

Once the historical backlog has been removed, delete one daily retention
batch:

```cron
15 3 * * * flock -n /tmp/upat-retention.lock sh -c 'cd /opt/upat-nzc-mqtt-db && docker exec -i iot_postgres psql -U postgres -d iot_db -v delete_batch_size=500000 < db/maintenance/upat_retention_batch.sql' >> /var/log/upat_retention.log 2>&1
```

Check the watermark and lag with:

```sql
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
```
