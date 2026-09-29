# Hourly Shelly energy aggregation

Calculates hourly imported Wh from Shelly cumulative counters. Pro 3EM uses
import-counter deltas; plugs subtract returned-energy deltas from absolute
energy. Missing or inconsistent hours remain unavailable; replay is idempotent.

From the repository root:

```sh
# Build
docker compose --profile jobs build aggregate-energy

# Preview only: reads the configured database
docker compose --profile jobs run --rm aggregate-energy --dry-run

# Offline counter tests
python -m pytest tests/test_shelly_counter_contract.py
```

Normal runs write to the configured database. `SHELLY_COUNTER_START` limits the
historical replay boundary; production keeps `2026-09-07T08:00:00+00:00`.
`OPEN_METEO_TIMEZONE` controls working-day/hour flags (default `Europe/Athens`).

VPS: `upat-aggregate-energy.timer` runs at minute 02 of every UTC hour, preserving
the former hourly cron schedule across DST. Its service retains the existing
`/var/lock/energy-aggregator.lock` and PostgreSQL advisory lock. The release image
is pinned through `/etc/upat-nzc/aggregate-energy.env` and `compose.release.yml`.
Use `systemctl start upat-aggregate-energy.service` for manual writes and
`journalctl -u upat-aggregate-energy.service` for logs.

Active on the VPS since 2026-09-29, from source `56c5d83`. The first scheduled
execution on 2026-09-30 at 00:02 Europe/Athens completed with exit 0 and refreshed
36 hourly rows across the three-hour replay window. Counter values and working
flags matched; rows before the cutover and outside the replay stayed unchanged.
Receipts, exact image ID, previous configuration, hourly-row backups and the
guarded rollback script are in `/opt/upat-aggregate-energy-release-20260929/`.
