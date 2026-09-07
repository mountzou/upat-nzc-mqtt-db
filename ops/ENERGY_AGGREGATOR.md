# Scheduled Shelly energy aggregator

Production installation cleaned up on 7 September 2026. This changes deployment
inputs only; `counter_delta_v1` and previously stored energy are unchanged.

## Current entrypoint

Root cron runs at minute 02 of every hour:

```cron
2 * * * * /opt/upat-nzc-mqtt-db/ops/run-energy-aggregator.sh >> /var/log/energy_aggregator.log 2>&1
```

The launcher owns the existing nonblocking lock
`/var/lock/energy-aggregator.lock`, so both cron and explicit launcher invocations
use the same lock. The worker's PostgreSQL advisory lock is unchanged.

The launcher uses one production Compose file, an explicit project directory and
project name, and the existing protected root `.env`. It executes only the
`energy-aggregator` service, with `--rm --no-deps --pull never -T --interactive=false`.
It accepts no arguments. Do not run it for a health check: it is the real writer.

## Canonical configuration

`docker-compose.prod.yml` now declares:

- the installed image `sha256:c16ade3bf7365c427ef44f261d4fe9a5b06ad3b50e485a58cc3caa01b9d7ffa7`;
- the `jobs` profile, excluding aggregation from an ordinary unprofiled `up`;
- no production `build` entry, so this job selects the reviewed image;
- fixed `SHELLY_COUNTER_START: "2026-09-07T08:00:00+00:00"` (11:00 Athens);
- the same database values, now interpolated from the root `.env` rather than a
  second copied credentials file.

The cutover is the historical method transition, not the installation date or a
value to advance on every deployment. Changing it requires a separate data
review. The effective worker environment was compared byte-for-byte by key/value
with the previous scheduled worker and matched. The local and VPS production
Compose files now match, including this previously divergent setting.

The development `docker-compose.yml` remains unchanged, with its local build and
optional variable mapping. Do not substitute it for the production job.

## Verification boundary

Preparation launched a disposable container with an explicit Python entrypoint
override. It verified deployed source hashes/cutover and executed only `SELECT 1`
in a read-only PostgreSQL transaction. It did not import or execute the aggregator
entrypoint, calculate energy or write hourly rows.

After installation, checks verified the exact cron replacement, source hashes,
all canonical Compose services, unchanged API hash, existing containers/mounts
and API/database health. An intentionally incorrect cutover was rejected by the
read-only verifier. Profiled stopped historical job containers are not treated
as the active scheduler image by that verifier.

At installation, the first natural run with this launcher was scheduled for
23:02 Athens on 7 September. The installation receipt does not include that
run's outcome. Verify subsequent runs through bounded `counter_delta_v1` log
receipts and persisted hourly rows; do not manually invoke the writer merely
to force an acceptance run.

## Rollback and retained history

Verified backups and `rollback-ready.json` are in
`/opt/schoolheroz-aggregator-installation-cleanup-20260907`.
Under the same writer lock, check for intervening changes, restore the saved
Compose and root crontab, and remove the new launcher only if its hash matches.
No service recreation or data rollback is part of this installation rollback.

The old `scheduled-aggregator.sh`, `run-aggregator.sh` and `aggregator.env` under
`/opt/schoolheroz-counter-final-20260907` remain unchanged for recovery/history;
they are no longer active scheduler inputs. The obsolete warm-up branch is thus
removed from the active execution path. The stopped historical
`energy_aggregator` container was retained and is not the scheduled worker.

Evidence: `../backups/aggregator-installation-cleanup-20260907/REPORT.md`.
