# Shelly cumulative energy contract

The source commit that adds this document records the counter schema, ingestor
and hourly worker. The matching API and production scheduler configuration are
recorded in subsequent commits. Production notes below describe the September
2026 rollouts; this source-recording step is not an instruction to apply the
migration or activate a writer again.

Final agreed policy: `counter_delta_v1`, hourly counter differences, boundary
observations within ±60 seconds, no veto based only on message-gap length.
The public `consumption.history.v1` response remains unchanged.

## Source data

`shelly_energy_counters` stores cumulative Wh per device/channel with aware
receiver timestamps. It is written atomically with existing raw/telemetry data.
Pro 3EM `emdata:0` provides imported `a/b/c_total_act_energy` and separate returned
Wh. Each assigned phase is counted once. Plug `switch:0` provides absolute
`aenergy.total`; subtract the increase in `ret_aenergy.total` when present.
Without returned counters, the current plug installations are assumed to be
consumption-only. Never use `by_minute` mWh or `minute_ts` as a total-Wh snapshot.

Sources: [EMData](https://shelly-api-docs.shelly.cloud/gen2/ComponentsAndServices/EMData/#status),
[Switch](https://shelly-api-docs.shelly.cloud/gen2/ComponentsAndServices/Switch/#status).

## Accounting

For each elapsed hour, use the nearest counter observation within 60 seconds
of each boundary. Ties select the earlier observation. Adjacent hours reuse
the same boundary; no interpolation, power integration or double counting.
The receiver timestamp approximates the counter instant, as explicitly agreed.
UTC elapsed-hour alignment preserves distinct autumn hours. The API continues
to group Athens calendar days of 23/24/25 hours and return kWh.

All available counters between the selected boundaries are checked for finite,
nonnegative values, counter shape changes, decreases/reset and conflicts.
An intermediate gap does not invalidate the hour when the boundaries and
available counters are consistent. This assumes counter continuity where no
reset is observed; a completely hidden reset cannot be ruled out.

A missing boundary or observed inconsistency makes the channel-hour unavailable.
Actual zero remains zero. A partly observed Pro 3EM row retains its valid phases
and NULLs for the unavailable ones; total is NULL unless all phases are valid.
An entirely unavailable device-hour has no row. Replay removes a stale previous
post-cutover result if new observations invalidate it. The unchanged API derives
complete/partial/missing coverage from available hourly values.

## Minimal persistence and diagnostics

Store counters and calculated hourly energy only. There is **no per-hour device
quality/diagnostic table**. Reasons and sample timing are transient calculation
results; the worker emits bounded aggregate run counts. Deployment records and
the configured cutover identify the method transition.

Long outages, repeated connectivity issues and resets belong to consolidated
operational diagnostics. This change does not introduce minute-level incident
rows, alerts or a new anomalies endpoint. Existing diagnostics remain separate.

## Safe activation

1. Back up exact live images/configuration. Apply additive migration 015 before
   activating the ingestor; existing tables/values are not rewritten.
2. Start counter collection and verify all communicating meters. Keep old hourly
   aggregation active during warm-up.
3. Choose an aligned future `SHELLY_COUNTER_START`, after collection has begun.
   Check candidate results and set the new scheduler/image with no writer overlap.
4. The new worker waits past the ±60s boundary before considering an hour closed,
   rechecks the last three closed hours, and refuses writes before cutover.
5. Verify counter freshness, scheduled-run result, hourly values and API coverage.
   The first complete new hour becomes verifiable only after its ending boundary.

The existing cron flock and a PG advisory lock serialize the new writer.
Explicit replay is bounded to seven days; default execution needs the configured
cutover. `--dry-run` uses a read-only database connection. Pre-cutover historical
energy remains unchanged; historical conversion requires a separate reviewed
backfill and rollback plan.

Rollback: stop the new writer, restore the backed-up image/configuration and any
affected backed-up hourly rows before resuming. Keep newly collected counters
and the additive table. Ingestor rollback restores its prior image. Do not deploy
an entire dirty checkout or overwrite unrelated live Compose configuration.

## Retired contract

The public `/energy/devices/{device_id}/pro3em-energy` estimate, its exclusive
SQL raw-power integration, schemas, interval dependency and unused consumer
helper were removed from the live VPS API on 2026-09-07. Device telemetry, school insights
and internal hourly readers still serve separate active uses. Production state
and evidence are recorded separately in the rollout report.

Current rollout receipt: `../backups/shelly-counter-final-20260907/REPORT.md`.
First complete counter hour: 2026-09-07 11:00–12:00 Athens; first cron 12:02.
The initial rollout receipt records this timed acceptance as pending.

## VPS source alignment

On 7 September 2026 the VPS `energy-aggregator/main.py`, `counter_energy.py` and
`Dockerfile` were aligned with the recorded tested counter release. Requirements
already matched. This was a guarded source-only transfer with verified backups;
no job, build or restart was performed. Cron still uses the explicit release
image and private `aggregator.env`, with the original cutover unchanged. Scheduler
simplification and Compose reconciliation remain separate work.
See `../backups/aggregator-source-alignment-20260907/REPORT.md`.

## Current scheduler installation

The subsequent installation cleanup is active: cron now calls the repository
launcher, with the same hourly schedule, flock, pinned image and historical
cutover. Production Compose is the job configuration; the root `.env` supplies
its database credentials. Legacy migration scripts/env remain as recovery
artifacts only. See [ENERGY_AGGREGATOR.md](ENERGY_AGGREGATOR.md) for operation,
rollback and the first natural-run verification boundary.
