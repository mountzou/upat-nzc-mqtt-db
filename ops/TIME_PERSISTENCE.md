# Telemetry time persistence — migration 014

This document records the 6 September persistence rollout. The source commit
that adds it contains the storage and ingestion foundation; the matching API,
consumer and deployment changes are recorded in subsequent commits. It is not
a standalone deployment release or an instruction to rerun migration 014.
The image IDs and validation results below describe that historical rollout.

Status on 6 September 2026: **migration 014 is live and verified on production.**
The second attempt committed successfully with 657.17 seconds (10 minutes,
57 seconds) of application interruption, within the authorized 15-minute window.
The first attempt's controlled rollback is retained below as historical evidence.
Client cleanup was the next phase at that checkpoint.

## Contract

- UPAT and Shelly measurement instants, raw-message times, device creation
  times, and UPAT rollup starts use `TIMESTAMPTZ`.
- Existing offset-free telemetry values represent UTC. Migration 014 casts
  them under `SET LOCAL TIME ZONE 'UTC'`, preserving their absolute instants.
- PostgreSQL stores instants independently of display timezone. The database
  default for new sessions and the API connection timezone become
  `Europe/Athens`; returned instants therefore include the relevant offset.
- PV actuals already use `TIMESTAMPTZ`. Database constraints now enforce that
  `local_date` equals the Athens calendar date of `observed_at` in both the
  plant and device reading tables. No PV measurement is rewritten.
- TTN validates that its source timestamp includes an offset before writing.
  The original timestamp string, including subsecond precision, is preserved.
  Shelly already constructs aware UTC datetimes; PV persistence already
  validates both awareness and source epoch correspondence.
- Internal history and energy requests retain their offset. Existing external
  offset-free request bounds keep their UTC interpretation at the shared
  `parse_telemetry_bound` compatibility boundary.
- Fixed-duration history buckets retain their existing UTC origin. Defining
  `interval=day` as an Athens calendar day is a separate API behavior change.
- Forecast wall-clock timestamps in weather/PV forecasting are outside this
  migration of measured telemetry. Calendar dates and school schedules remain
  dates/times, not artificial measurement instants.

## Verified production baseline

PostgreSQL 15.17, timezone `Etc/UTC`, database approximately 17.34 GB, VPS free
space approximately 9.8 GiB at inspection. No dependent public views or
materialized views were present. The live source hashes matched the scoped
local prechange snapshot.

The restored snapshot contains 59,778,036 Shelly measurement rows. Ten audited
tables contain 67,550,130 rows in total. The migration alters ten timestamp
columns across eight UPAT/Shelly tables, and adds two PV calendar constraints.
The oldest/latest 4,000 sampled UPAT raw timestamps all matched source UTC
instants. All 17,907 plant and 63,634 device PV rows had consistent Athens dates.

## Evidence retained locally

Private, gitignored directory:
`/Users/mountzou/upat-nzc-mqtt-db/backups/time-persistence-20260906/`

- `pre-migration.dump`: complete custom-format PostgreSQL backup, 976,520,614
  bytes, SHA-256
  `0355bce49f409ba5b50fcaf5cfb1000bcd2c89bde6d222a49278ad9182e206a0`.
- `backup-receipt.json`, `restore.log`: dump and isolated restore succeeded.
- `data-before.json`, `data-after.json`, `data-rollback.json`: exact counts,
  null counts, min/max/summed timestamp epochs, measurement checksums, rollup
  sample totals/checksums, PV calendar checks and object validity checks.
- `verification-comparison.json`: all audited values/instants match after
  migration and after rollback. All table heap files were preserved.
- `api-before.json`, `api-after.json`: 19 representative query results match
  after timestamp representations are normalized to the same instants.
- `full-migration-receipt.json`: full migration including statistics took
  72.78 seconds locally. `full-rollback-receipt.json`: rollback took 92.52 seconds.
  These are local rehearsal times, not production downtime guarantees.
- `api-all-db-tests.log`: 155 passed, plus 9 subtests, with real PostgreSQL
  checks enabled. `root-tests.log`: 30 passed, 16 unrelated optional tests
  skipped. Migration tests cover rollback, idempotence, failed-constraint
  rollback, nulls, preserved values, and the two distinct autumn 03:30 instants.
- `time-persistence.patch`, `release-manifest.json`,
  `time-persistence-release.tar.gz`: isolated reviewed scope. Pre-existing
  unrelated working-tree changes are not part of this release.

## Production rollout checkpoint

`ALTER COLUMN TYPE` requires an exclusive table lock. PostgreSQL 15 preserves
the UTC heap here, but rebuilds timestamp-bearing indexes. Shelly indexes are
approximately 7.48 GB on the live database; recheck free space and temporary
sort/WAL headroom immediately before execution.

Both MQTT consumers process inserts in their receive callbacks, subscribe at
QoS 0, and use a 60-second keepalive. A long blocked insert can interrupt
collection. A rollback protects persisted data; it cannot recover MQTT messages
that never reached persistence. The first accepted two-to-three-minute window
was insufficient and the attempt was safely cancelled. The user then explicitly
authorized up to 15 minutes of collection interruption and temporary API
unavailability, with cancellation around minute 12. That second attempt
succeeded; see the successful rollout evidence below.

For a subsequent rollout:

1. Recheck source/image hashes, DB timezone, free space, active jobs and the
   absence of long transactions. Refresh the off-host backup for the cutover.
2. Prepare API and TTN images from the exact running images with only the
   scoped source overlays; retain the old images and all changed live files.
3. Coordinate the API/ingestors and rollup/retention/energy jobs with the
   agreed collection strategy. Do not restart PostgreSQL or remove its volume.
4. Run `db/migrations/014_telemetry_timestamptz.sql` using `psql` with
   `ON_ERROR_STOP`. The script uses a five-second lock-acquisition timeout and
   a transaction; any failure rolls back the schema and timezone setting.
5. Install the matching API, TTN and maintenance files and recreate only the
   changed application services. Reconnect consumers after the DB timezone
   changes. No Caddy, Render, web or iOS release is required for this phase.
6. Verify column types, offset-bearing reads, source epoch equality, PV dates,
   index validity, query-result parity, fresh ingestion and API health.

If application acceptance fails after commit, coordinate the same collection
strategy, run `db/maintenance/rollback_014_telemetry_timestamptz.sql`, restore
the preceding API/TTN/maintenance files and images, and verify ingestion.
The inverse migration preserves values written after the forward migration.
Its database timezone reset is matched to the observed baseline; if that
baseline changes, revise the rollback before executing it.

## Client cleanup after persistence is live

Remove legacy offset-free *measurement* parsing from web
`frontend/src/lib/datetime/timezone.js` and the iOS `parseTimestamp` helper
after all corresponding endpoints are verified to return explicit offsets.
Review Render consumers under the same rule. Preserve shared Athens display
formatters, calendar-date handling and DST-aware duration calculations; these
encode domain meaning rather than repair ambiguous persistence.

## Bounded production attempt — 6 September 2026

- Fresh full off-host backup: `cutover-20260906T1259.dump`, 977,173,985 bytes;
  SHA-256 `bcd98a04aad48563293fd9ceb68417641915d1371c8ca6503cfe58a1c7e1fb2d`.
  Dump exit code and archive catalog verified. The earlier full backup had
  already passed isolated full restore, migration and inverse-migration checks.
- Exact live source hashes and both changed services' environment/commands/
  mount configuration were checked. Candidate API and TTN images were built
  from the current images without dependency changes and retained as staged
  artifacts; neither candidate replaced a live service.
- Rollup, retention and energy-aggregator flock locks were held only during
  the cutover and released on exit. PostgreSQL and Mosquitto stayed running.
- Applications stopped starting at 13:07:04 UTC (16:07:04 Athens). The bounded
  execution copy used a 120-second statement timeout and an overall controller
  watchdog with a separate disk-headroom guard. The original migration source
  was not modified by this operational timeout override.
- On the Shelly index scan, observed progress was 127,635 of 737,766 blocks
  after approximately one minute of total application interruption. This
  could not fit the remaining budget, so the migration backend was explicitly
  cancelled before its timeout. PostgreSQL rolled back the transaction.
- Migration execution: 98.87 seconds. All previous application containers were
  running again at 13:08:50 UTC, 105.43 seconds after starting the pause.
  Fresh UPAT and Shelly raw rows were persisted at 13:08:52.531870 UTC and
  13:08:53.774178 UTC respectively. The number of unpublished/unreceived
  QoS-0 messages cannot be reconstructed from the database.
- All ten legacy column types, `Etc/UTC`, original runtime images and live
  source hashes were verified restored/unchanged. PostgreSQL retained its
  8 June process start time. No production restore, Caddy change, or web/
  Render/iOS deployment occurred.
- Private controller, logs and receipts: local backup directory above and
  VPS `/opt/upat-time-persistence-20260906T1300/`. The transient cutover unit
  exits with status 2 on the intentional abort; it is not a recurring job.

After the first attempt, a capture/replay strategy was proposed to retain MQTT
arrivals during longer database locks. Such a spool must preserve original
receipt instants and prevent duplicate inserts. The user instead authorized the
longer collection gap recorded below, allowing the second attempt to proceed.
No spool was introduced.

### Final acceptance after the controlled rollback

All 67,566,209 rows in the fixed comparison scope retained identical counts and
timestamp epoch totals. UPAT/Shelly value checksums and all heap files match.
PV exact numeric sums and row checksums match the isolated prechange restore;
a 0.0000013113 variation in an unordered double-precision SUM was identified
as aggregation-order rounding and independently resolved by these exact checks.
There are zero invalid indexes, zero unvalidated constraints and no unexplained
data differences. Twelve HTTP checks passed: internal DB health, public auth
and school scoping, catalog, environment latest/history, energy latest/history,
and PV aggregates. The first fresh MQTT writes arrived within four seconds of
the old containers restarting. All previous runtime images remain live.

Local production evidence is in `backups/time-persistence-20260906/vps/`;
`verification-after-abort.json` is the final integrity receipt. The failed-state
marker of the intentional one-shot controller abort was cleared after recovery
verification; the controller is not scheduled to run again.

## Successful production rollout — 6 September 2026

The same rehearsed 13-file runtime/SQL release was applied. The execution-only
settings were `statement_timeout=710s`, overall cancellation at approximately
720 seconds of interruption, and `SET LOCAL maintenance_work_mem='256MB'`.
The database-wide memory setting remained 64 MB afterward. A separate
disk-headroom guard and original files/images remained available throughout.
No dependency upgrades, PostgreSQL restart, broker restart, Caddy change, or
web/Render/iOS deployment was part of this persistence release.

- Application pause began **13:31:34 UTC / 16:31:34 Athens**.
- Migration committed at **13:42:28 UTC / 16:42:28 Athens**, exit code 0.
  SQL execution took **648.66 seconds**.
- New API and TTN containers were running by **13:42:32 UTC / 16:42:32 Athens**;
  Shelly had already restarted at 13:42:28. Total controller-measured pause:
  **657.17 seconds**. New raw rows were verified for both sources immediately
  after service resumption. The missed QoS-0 message count is not knowable.
- All ten target columns are `timestamp with time zone`; database default and
  API connections report `Europe/Athens`. Both PV Athens-date constraints are
  validated. Winter/summer reads show +02:00/+03:00 respectively.
- The fixed comparison scope of **67,578,412 rows** retained exactly equal row
  counts, timestamp epoch totals, UPAT/Shelly measurement checksums, rollup
  checksums and exact-numeric PV sums. All table heap files were preserved;
  zero invalid indexes and zero unvalidated constraints remain.
- All **4,000** sampled oldest/latest UPAT raw timestamps match source
  `received_at` instants.
- **15** fixed historical query snapshots are semantically identical; 14
  match exactly. Two phase-energy floating-point sums differ by only
  1.4210854715202004e-14 Wh, with no timestamp or persisted-data difference.
- **12 HTTP checks passed**, including strict absence of offset-free
  timestamps in checked measurement responses. The first energy-latest check
  timed out at 30 seconds during the full integrity scan; after that scan,
  it passed in 15.77 seconds. Its full-history aggregation is a separate
  performance concern, not resolved by migration 014.
- The existing signed-in Chrome `school_10` tab was refreshed successfully:
  current readings loaded, the summary range displayed 15:49–16:49 Athens,
  and the live UPAT timestamp displayed 16:48. The pre-existing AC-floor
  non-live and incomplete simulation statuses remained.
- UPAT scheduled rollups resumed after release of the maintenance lock.
  API, TTN, Shelly, PostgreSQL, Caddy and Mosquitto were all running with zero
  restart-loop counts; PostgreSQL retained its 8 June start time.

Private evidence directory:
`backups/time-persistence-20260906/attempt-15m/` locally;
`/opt/upat-time-persistence-20260906T1325/` on the VPS.

Fresh off-host backup: `pre-cutover.dump`, **977,577,305 bytes**, SHA-256
`739afb044cbe900df2af86648e1462a6cd18689b8021d740a7f7f1e784942dfb`.
Dump and archive-catalog checks passed; the preceding full production snapshot
had already passed isolated restore, forward migration and inverse migration.

Live images:
- API: `sha256:6dd6b2444233060dd5aecf9667f4b10a707251c8b67d0594c027818dd24b2c8e`.
- TTN: `sha256:9a7d0f6bcb0f2dafed7e2a0487f1c371d018a492a9e78c730baf552c04e0d3bd`.

Original API/TTN images and scoped source backups are retained with the inverse
migration. A post-commit inverse migration requires its own coordinated window;
it is not assumed to fit the brief pre-commit cancellation recovery budget.
