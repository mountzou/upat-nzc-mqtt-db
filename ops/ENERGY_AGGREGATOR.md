# Scheduled Shelly energy aggregator

The source and Compose service are `jobs/aggregate-energy` and `aggregate-energy`.
`upat-aggregate-energy.timer` runs every UTC hour at minute 02. It replaced the
previous root cron on 2026-09-29 without changing the hourly accounting or replay
policy. The first scheduled execution on 2026-09-30 at 00:02 Athens completed
with exit 0; all 36 refreshed hourly rows matched their counter inputs.

The oneshot service preserves `/var/lock/energy-aggregator.lock` and the worker's
PostgreSQL advisory lock. Its versioned release overlay selects only the image
pinned in `/etc/upat-nzc/aggregate-energy.env`; credentials come from the existing
protected root `.env`. Use `systemctl start upat-aggregate-energy.service` for
an intentional manual run.

## Configuration

- Production `SHELLY_COUNTER_START` stays `2026-09-07T08:00:00+00:00`.
- `OPEN_METEO_TIMEZONE` controls working-period flags; default `Europe/Athens`.
- Each run rechecks three closed elapsed UTC hours after the boundary tolerance.
- The `jobs` profile excludes aggregation from an ordinary Compose `up`.

## Verification and rollback

Use service exit status, journal invocation IDs and persisted hourly values as
execution evidence. Counter inputs must reproduce the stored per-phase/plug
values, missing-hour policy and working-period flags. Historical rows before
the fixed cutover must remain unchanged.

`--dry-run` previews through a read-only database connection. Starting the service
is a real write; do not run it merely for a routine health check.

The 2026-09-29 release receipt, prior configuration/crontab, hourly-row backups
and guarded rollback script are in `/opt/upat-aggregate-energy-release-20260929/`.
Rollback stops the timer and restores the exact previous scheduler/configuration
under the same writer lock. Review any hourly-row restoration separately.

Earlier installation receipts and sources under
`/opt/schoolheroz-aggregator-installation-cleanup-20260907` and
`/opt/schoolheroz-counter-final-20260907` remain recovery artifacts.
See [the counter contract](SHELLY_COUNTER_ENERGY.md) for accounting details.
