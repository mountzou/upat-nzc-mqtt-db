# Scheduled Shelly energy aggregator

The source and Compose service are `jobs/aggregate-energy` / `aggregate-energy`.
`upat-aggregate-energy.timer` runs every UTC hour at minute 02.
The oneshot keeps `/var/lock/energy-aggregator.lock` and the worker's PostgreSQL
advisory lock. Its release overlay selects the image pinned in
`/etc/upat-nzc/aggregate-energy.env`; credentials come from the protected root
`.env`.

## Configuration

- Production `SHELLY_COUNTER_START` stays `2026-09-07T08:00:00+00:00`.
- `OPEN_METEO_TIMEZONE` controls working-period flags; default Europe/Athens.
- Each run rechecks three closed elapsed UTC hours after the boundary tolerance.
- The `jobs` profile excludes aggregation from an ordinary Compose up.

See [the counter contract](SHELLY_COUNTER_ENERGY.md) for accounting and
[the job guide](../jobs/aggregate-energy/README.md) for commands.
Starting the service writes hourly energy; `--dry-run` uses a read-only connection.

## Verification and recovery

Check service exit status, journal invocation and persisted hourly values.
Counter inputs must reproduce per-phase/plug values, unavailable-hour handling
and working-period flags. Pre-cutover history must remain unchanged.

Recovery material is retained at `/opt/upat-aggregate-energy-release-20260929/`.
Its guarded scheduler rollback stops the timer and uses the same writer lock.
Review the saved image/configuration against current schema and contracts before
reuse; hourly-row restoration requires a separate data reconciliation.
Retain counters and newer readings. Application/storage compatibility follows
[current recovery requirements](shelly-compact/RECOVERY.md).
