# Staged Shelly measurement migration

Status: historical staged plan. The production stage 3 activation completed on
2026-09-16; see `PRODUCTION-STAGE3-20260916.md`. Do not rerun the staged
controllers against the live database without a new review.

## Scope and invariants

Only Shelly measurement storage and its existing API history/latest readers change.
Device metadata, raw MQTT messages, energy counters, hourly energy tables, UPAT,
PV and simulation jobs retain their existing behavior. Raw float8 values, IDs,
units and timestamptz values are copied without rounding. Only Shelly API averages
use `ROUND(AVG(value::numeric),1)::double precision` after activation. Ties round
away from zero; a previous floating-point result can differ by 0.1.

PostgreSQL remains under `upat-postgres-volume.service`, with data on
`/mnt/HC_Volume_106884142/pgdata`. No PostgreSQL restart, table rename, legacy
DELETE, TRUNCATE, DROP, VACUUM FULL or legacy index rebuild is part of this rollout.
Never run Compose down or a whole-stack Compose up. Only recreate an explicitly
selected application with `--no-deps --no-build`. Keep the existing external network.

The old table owns the shared ID sequence. **Do not drop the old table**: a later,
separately approved retirement must first transfer sequence ownership and audit all
remaining dependencies. Retaining the old table means storage initially increases;
space savings are realized only after its separately reviewed retirement.

## Immutable release and preflight

Build overlay images from a clean git archive of the recorded code commit, using:

- API baseline `sha256:d27069f42d0f4cb647d8011a19dfbf81a7dcb71a140b493f033ed847eb788856`.
- Ingestor baseline `sha256:540f7d5f5d4e277593a3fea2e9bccd6dc96a3817483fd2fa5d2d63ce39f984dd`.
- Dockerfiles in this directory. No package upgrades or dependency installation
  occur in these production candidates. Record base ID, source commit and result ID.

Before production activation, confirm the user-approved stage, a recent verified
logical backup outside the VPS, available restore evidence, current application
source/image IDs and the current database schema. Do not build from the dirty VPS
checkout or overwrite it. Store a versioned release in its own directory. Preserve
private Compose/environment rollback files without printing secrets.

Run the existing host Volume preflight and Compose ownership validator. Capture
container IDs, start times, PostgreSQL postmaster start time, cluster ID, free space,
API health, newest Shelly/UPAT readings and counter progress. Expected cluster ID:
`7618955918777626661`. Inspect active writers, views, scheduled scripts and ORM/raw
SQL consumers; the current repository routes Shelly measurements through the two
modified API readers. Energy aggregation uses separate counter/hourly tables.
Any unknown direct writer blocks checkpoint capture until accounted for.

## Phase A: prepare and start atomic dual writes

1. Run `migrate.py prepare` once. It creates only `shelly_compact` objects. Existing
   objects cause transaction failure instead of silently accepting schema drift.
2. Pause/drain the Shelly ingestor and all other identified measurement writers.
   Keep PostgreSQL, API, TTN and unrelated jobs running.
3. Run `capture --writers-paused --direction forward`. The one-second lock timeout
   must succeed after writers drain. It obtains a source SHARE lock before taking
   the maximum ID, preventing a delayed pre-checkpoint transaction being skipped.
   On failure, restore the old ingestor and investigate; do not kill database sessions.
4. Start the verified ingestor candidate with `SHELLY_MEASUREMENTS_WRITE_MODE=dual`.
   Keep API storage/rounding `legacy`. Each MQTT transaction commits both copies,
   raw messages and counters together, or rolls them all back. Sequence gaps after
   rollback are harmless. Check new IDs and exact values in both schemas, ingestion
   progress and logs before continuing. A short MQTT gap during this application
   recreation was accepted by the user; durable replay is not claimed.
5. On a failed candidate, restore the exact original ingestor image/config. The old
   table remains authoritative. Before retrying after legacy-only writes, pause it
   again and extend the forward checkpoint with `capture`.

## Phase B: bounded historical backfill and index

Run the utility in a disposable migration container using the ingestor candidate,
with the existing database network and a **read-only** mount of the verified Volume
filesystem at `/space-check`. The container command is
`python /release/ops/shelly-compact/migrate.py ...`. Supply the DSN privately via
`SHELLY_MIGRATION_DSN`, never in a log or shell command line.

All production invocations require `--production --expected-system-id
7618955918777626661`. Copy/index additionally require
`--space-check-path /space-check`. The mount must refer to the PostgreSQL Volume,
not the root disk. Start with `copy --batch-size 10000 --pause 0.5` and observe load
before increasing. Each completed batch and its cursor commit together. Interruptions
roll back only the current batch; resume with the same command. Existing IDs are
accepted only when their complete raw fields match.

The guard checks before each copy batch: at least 20 GiB free, retained WAL no more
than 4 GiB. Monitor Volume/root free space, PG activity, ingestion age and API latency
throughout. Stop the copy if live service latency degrades, even if capacity is ample.
Do not delete WAL or change durability settings. Total WAL generated over a run can
exceed 4 GiB because PostgreSQL normally recycles it; the guard limits retained WAL.

After forward progress reaches its fixed high-water mark, run `index`. It builds
`measurements_series_time_covering` CONCURRENTLY and runs ANALYZE on only the new
objects. The primary key protects IDs throughout backfill; the large read index is
built afterwards for a denser result. API stays on legacy during this step. Reserve
extra space for the index build and temporary sort files. The capacity guard runs
before the build; continue external space/load monitoring during it.

If interrupted, a concurrent build may leave an invalid index. The helper refuses
an unexpected/invalid existing index. Inspect its definition/state and confirm API
still uses legacy before separately removing that exact invalid new index and
retrying. Do not touch any legacy indexes. An already valid expected index is safe
to reuse. Do not switch readers if any required index is missing or invalid.

## Phase C: verification and API cutover

`verify` streams the six original fields ordered by ID from both layouts in one
READ ONLY REPEATABLE READ snapshot, compares SHA-256/byte counts, row counts and ID
bounds, and checks new index/constraint validity. Concurrent dual inserts are atomic
and therefore visible on both sides or neither side. This is a full-data comparison,
not a sample. Run it near the database to avoid sending the full raw dataset over SSH.

Then run authenticated API probes for latest/history, devices with empty results,
metric filters, minute/hour buckets, timezone/DST and user/school authorization.
Compare both layouts under the SAME decimal_1 policy. The existing unlimited-history
latest query remains potentially expensive; this change does not redesign that query.

After the verification gate and agreed activation checkpoint, recreate only the API
candidate with `READ_STORAGE=compact`, `ROUNDING=decimal_1`. Ingestor remains `dual`.
Observe live new measurements and hourly counter results through the normal application
API. Rollback is a targeted API recreation with `READ_STORAGE=legacy`; keep
`ROUNDING=decimal_1` for the same approved averaging semantics. An exact original-image
rollback also restores the original rounding behavior.

The override file requires explicit images and modes. Use immutable SHA256 IDs in
the resolved Compose model. Persist the approved image/flags in the canonical
production Compose configuration atomically and narrowly, so a later plain targeted
Compose command cannot revert the deployment. Compare the complete resolved model:
only the Shelly ingestor/API image IDs and their three declared flags may differ.
Keep the development Compose file out of the production directory. Re-run the
PostgreSQL ownership guard and read back all effective image IDs and modes.

## Phase D: compact-only writes and rollback

The approved stage-3 runner changes only `SHELLY_MEASUREMENTS_WRITE_MODE` from
`dual` to `compact`, reusing the exact image and leaving the API and PostgreSQL
untouched. Audit active consumers first. Complete a fresh backup, local restore,
and Chris checksum readback before activation.

Run `stage3.py preflight` and `stage3.py verify` from the immutable release. The
full verification uses one read-only repeatable-read snapshot while dual ingestion
continues. `stage3.py activate` stops/drains only the ingestor, acquires short SHARE
locks on both measurement tables, and verifies the entire tail after the original
stage-1 drained-writer barrier. This includes IDs allocated before the recent
snapshot but committed afterwards. Together with the full verified prefix and
our audited append-only writer, this certifies equality through the final ID.
Unexpected historical updates require a new review; this proof assumes immutable
measurements. A reverse progress row is initialized at that certified ID without
recopying all historical rows. The canonical Compose flag is then installed and
only the ingestor is recreated. Use `stage3.py status` afterwards.

Rollback is **not** an environment toggle. `stage3.py rollback` stops the ingestor,
extends the reverse checkpoint, copies only the newer compact tail in bounded
batches, and verifies binary tail equality before restoring the saved dual Compose
configuration. It leaves API reads on compact. Recovery conflicts leave the writer
stopped instead of silently accepting different measurements. This path is tested
locally; production rollback is only executed if activation fails or separately
requested. Preserve the shared sequence value and its current ownership.

The old table and all raw messages remain until a separate cleanup approval. At every
phase, verify unchanged PostgreSQL container ID/start time, Volume source, healthy
API, fresh ingestion and counter progress. Record receipts with timestamps, commit,
image IDs, checkpoint, full integrity result, sizes and actual observed latency.

## Local validation

The PostgreSQL integration tests require an explicitly isolated database:
`SHELLY_COMPACT_TEST_DSN='host=127.0.0.1 port=... dbname=compact_fixture user=...'
python -m pytest tests/test_shelly_compact_postgres.py`. They reset only that fixture.
The full-size rehearsal uses a separate clone of the verified post-Volume backup,
with the exact production PostgreSQL 15.17 amd64 image. It never connects to production
for writes. Read-only audits against the VPS are recorded separately.

PostgreSQL references: [snapshot isolation](https://www.postgresql.org/docs/15/transaction-iso.html),
[sequence behavior](https://www.postgresql.org/docs/15/functions-sequence.html).
