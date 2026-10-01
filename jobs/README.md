# Scheduled jobs

This directory is the home for application workloads that run on a schedule. Each migrated workload gets its own `jobs/<job-name>/` directory containing its implementation, tests, and versioned systemd service/timer definitions.

Existing workloads remain in their current locations until they are migrated. Continuously running services do not belong here.

Use lowercase, hyphen-separated names. Forecast jobs start with `forecast-`
so they sort together. Directory and Compose service names omit the project
prefix; VPS job and systemd unit names use `upat-<job-name>`.

| Job | Directory / Compose service | VPS name | Migration status |
| --- | --- | --- | --- |
| Weather forecast | `forecast-weather` | `upat-forecast-weather` | Active since 2026-09-29; manual and scheduled runs/data verified |
| Shelly energy aggregation | `aggregate-energy` | `upat-aggregate-energy` | Active since 2026-09-29; first scheduled run/data verified |
| PV telemetry collection | `collect-pv` | `upat-collect-pv` | Local migration complete; VPS still uses `upat-pv-ingestor` until cutover |
| PV forecast | `forecast-pv` | `upat-forecast-pv` | Active since 2026-09-29; manual and scheduled runs/data verified |

See [aggregate-energy](aggregate-energy/README.md), [collect-pv](collect-pv/README.md),
[forecast-weather](forecast-weather/README.md) and [forecast-pv](forecast-pv/README.md)
for local commands.

## Migration

Move one job at a time:

1. Verify its deployed schedule, configuration and output.
2. Prepare `upat-<job-name>.service` and `.timer` in its directory, preserving
   the schedule, timezone and overlap protection.
3. Build and deploy the reviewed source, replacing the previous scheduler
   without duplicate runs.
4. Verify execution and persisted output before marking the migration complete.
