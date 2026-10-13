# Shelly cumulative energy contract

Policy: `counter_delta_v1`, hourly counter differences with boundary observations
within ±60 seconds and no veto based only on message-gap length.
Public [consumption history](CONSUMPTION_HISTORY.md) retains its kWh contract.

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
operational diagnostics. Counter aggregation does not create minute-level
incident rows, alerts or an anomalies endpoint. Diagnostics remain separate.

## Operation and recovery

The current schema includes `shelly_energy_counters` from migration 015.
The ingestor writes counters atomically with raw/compact telemetry.
The [scheduled aggregator](ENERGY_AGGREGATOR.md) retains the fixed production
cutover, flock and PostgreSQL writer locks. It waits beyond the ±60s boundary,
rechecks three closed hours and refuses pre-cutover writes. Explicit replay is
bounded to seven days; `--dry-run` uses a read-only connection.

An application change keeps the cutover and newly collected counters. Coordinate
the writer before restoring a verified compatible image/configuration pair;
hourly-row restoration or historical conversion needs separate reconciliation
that preserves newer history. See [current recovery requirements](shelly-compact/RECOVERY.md).

The former public pro3em-energy estimate is retired. Device telemetry, energy
insights and protected hourly readers remain active dependencies.
