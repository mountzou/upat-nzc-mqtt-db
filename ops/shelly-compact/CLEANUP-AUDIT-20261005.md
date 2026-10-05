# Post-retirement code/file audit, 2026-10-05

This was a read-only audit. No additional source cleanup, VPS file deletion,
container rebuild or deployment was performed.

## Confirmed live state

At 12:58 Europe/Athens, `public.shelly_measurements` was absent, the shared
sequence remained OID 16445, compact remained OID 121068, and new measurements
were reaching compact. The API and Shelly ingestor run image-packaged code with
no host source mounts. Both use `/opt/upat-nzc-mqtt-db/docker-compose.prod.yml`.

## Coordinated source cleanup

| Source | Obsolete part | Required preservation/change |
|---|---|---|
| `api/readers/shelly_storage.py` | Legacy table selection, legacy rounding and their env selectors | Keep compact reads and decimal-one averaging, plus UPAT's existing AVG semantics. The file is currently imported by main/latest readers; deletion requires changing its callers. |
| `mqtt/shelly-devices/measurements.py` | Legacy/dual write branches and mode selection | Preserve active compact insert, shared sequence allocation, dictionary resolution and transaction checks. Main still imports this writer. |
| `docker-compose.yml`, `docker-compose.prod.yml` | Storage/write/rounding selectors after code is made compact-only | Remove selectors together with code changes; the current dev defaults are legacy and must be coordinated with bootstrap changes. |
| `db/init.sql` | Legacy Shelly measurement table and its two indexes | Replace the Shelly bootstrap with independent sequence plus compact schema/tables/view/dictionary/index setup. Keep unrelated tables. Migration 017 currently depends on the old SERIAL-generated sequence. |
| PostgreSQL test fixtures | Legacy fixture tables and patched legacy/compact modes | Update fixture schemas while preserving concurrent writes, atomic rollback, numeric policy, timezone and DST behavioral coverage. |
| Current-state docs/comments | References to writing/reading the removed physical table | Update README and the ingestor comment; retain dated migration evidence. |

`shelly_measurements` passed by several callers is still a logical source key
resolved to the compact view. Removing every occurrence without changing the
shared dispatch would break callers. `shelly_measurements_id_seq` is an active
object, not an obsolete table reference.

`db/maintenance/rollback_014_telemetry_timestamptz.sql` and its reference in
`ops/TIME_PERSISTENCE.md` need separate review: they describe the previous
UTC-naive persistence contract across several sources, not only Shelly. As-is,
the script assumes the removed table. Retire or revise that recovery contract
explicitly instead of deleting a single table reference blindly.

Keep migrations 014/017 as historical schema/test inputs until a coordinated
replacement bootstrap and fixture plan is implemented. Do not delete the whole
compact integration suite: its concurrency, atomicity and API parity cases
remain useful.

## Whole historical-file candidates on the VPS

44 exact files, **405,409 bytes** total, were found in seven historical Shelly
release directories plus one leftover Dockerfile in the main VPS checkout.
These helpers had already been retired from the local repository. No candidate
release path appeared in inspected cron/systemd or host operational scripts.
No active process command/cwd or open descriptor referenced the release roots.
No candidate file is a current container mount or canonical build Dockerfile;
current build contexts use their standard Dockerfiles.

The list below is scoped to individual files. It does not approve removing whole
release directories, images, archives, backups or unrelated Compose inputs.

| Exact VPS path | Bytes |
|---|---:|
| `/opt/upat-nzc-mqtt-db/ops/shelly-compact/Dockerfile.ingestor` | 378 |
| `/opt/upat-shelly-compact-stage3-20260916/ops/shelly-compact/stage2.py` | 19743 |
| `/opt/upat-shelly-compact-stage3-20260916/ops/shelly-compact/compose.stage3.prod.yml` | 7362 |
| `/opt/upat-shelly-compact-stage3-20260916/ops/shelly-compact/stage1.py` | 15261 |
| `/opt/upat-shelly-compact-stage3-20260916/ops/shelly-compact/compose.stage2.prod.yml` | 7359 |
| `/opt/upat-shelly-compact-stage3-20260916/ops/shelly-compact/writer-cutover.py` | 4987 |
| `/opt/upat-shelly-compact-stage3-20260916/ops/shelly-compact/stage3.py` | 10569 |
| `/opt/upat-shelly-compact-stage3-20260916/ops/shelly-compact/migrate.py` | 14480 |
| `/opt/upat-shelly-compact-stage2-20260916/ops/shelly-compact/shadow-api.py` | 1111 |
| `/opt/upat-shelly-compact-stage2-20260916/ops/shelly-compact/stage2.py` | 17486 |
| `/opt/upat-shelly-compact-stage2-20260916/ops/shelly-compact/compose.stage1.prod.yml` | 7265 |
| `/opt/upat-shelly-compact-stage2-20260916/ops/shelly-compact/stage1.py` | 15261 |
| `/opt/upat-shelly-compact-stage2-20260916/ops/shelly-compact/compose.stage2.prod.yml` | 7359 |
| `/opt/upat-shelly-compact-stage1-20260916/ops/shelly-compact/copy-window.py` | 2598 |
| `/opt/upat-shelly-compact-stage1-20260916/ops/shelly-compact/compose.override.yml` | 696 |
| `/opt/upat-shelly-compact-stage1-20260916/ops/shelly-compact/build-local.sh` | 1246 |
| `/opt/upat-shelly-compact-stage1-20260916/ops/shelly-compact/compose.stage1.prod.yml` | 7265 |
| `/opt/upat-shelly-compact-stage1-20260916/ops/shelly-compact/stage1.py` | 15261 |
| `/opt/upat-shelly-compact-stage1-20260916/ops/shelly-compact/Dockerfile.api` | 311 |
| `/opt/upat-shelly-compact-stage1-20260916/ops/shelly-compact/Dockerfile.ingestor` | 370 |
| `/opt/upat-shelly-compact-stage1-20260916/ops/shelly-compact/migrate.py` | 14480 |
| `/opt/upat-shelly-compact-stage1-20260916/ops/shelly-compact/smoke-local-images.py` | 5267 |
| `/opt/upat-shelly-compact-stage2-20260916-r4/ops/shelly-compact/shadow-api.py` | 1111 |
| `/opt/upat-shelly-compact-stage2-20260916-r4/ops/shelly-compact/stage2.py` | 19743 |
| `/opt/upat-shelly-compact-stage2-20260916-r4/ops/shelly-compact/compose.stage1.prod.yml` | 7265 |
| `/opt/upat-shelly-compact-stage2-20260916-r4/ops/shelly-compact/stage1.py` | 15261 |
| `/opt/upat-shelly-compact-stage2-20260916-r4/ops/shelly-compact/compose.stage2.prod.yml` | 7359 |
| `/opt/upat-shelly-compact-stage2-20260916-r3/ops/shelly-compact/shadow-api.py` | 1111 |
| `/opt/upat-shelly-compact-stage2-20260916-r3/ops/shelly-compact/stage2.py` | 19185 |
| `/opt/upat-shelly-compact-stage2-20260916-r3/ops/shelly-compact/compose.stage1.prod.yml` | 7265 |
| `/opt/upat-shelly-compact-stage2-20260916-r3/ops/shelly-compact/stage1.py` | 15261 |
| `/opt/upat-shelly-compact-stage2-20260916-r3/ops/shelly-compact/compose.stage2.prod.yml` | 7359 |
| `/opt/upat-shelly-compact-stage2-20260916-r2/ops/shelly-compact/shadow-api.py` | 1111 |
| `/opt/upat-shelly-compact-stage2-20260916-r2/ops/shelly-compact/stage2.py` | 17538 |
| `/opt/upat-shelly-compact-stage2-20260916-r2/ops/shelly-compact/compose.stage1.prod.yml` | 7265 |
| `/opt/upat-shelly-compact-stage2-20260916-r2/ops/shelly-compact/stage1.py` | 15261 |
| `/opt/upat-shelly-compact-stage2-20260916-r2/ops/shelly-compact/compose.stage2.prod.yml` | 7359 |
| `/opt/upat-shelly-compact-stage3-20260916-r2/ops/shelly-compact/stage2.py` | 19743 |
| `/opt/upat-shelly-compact-stage3-20260916-r2/ops/shelly-compact/compose.stage3.prod.yml` | 7362 |
| `/opt/upat-shelly-compact-stage3-20260916-r2/ops/shelly-compact/stage1.py` | 15261 |
| `/opt/upat-shelly-compact-stage3-20260916-r2/ops/shelly-compact/compose.stage2.prod.yml` | 7359 |
| `/opt/upat-shelly-compact-stage3-20260916-r2/ops/shelly-compact/writer-cutover.py` | 5066 |
| `/opt/upat-shelly-compact-stage3-20260916-r2/ops/shelly-compact/stage3.py` | 10569 |
| `/opt/upat-shelly-compact-stage3-20260916-r2/ops/shelly-compact/migrate.py` | 14480 |

## Checkout/documentation boundaries

The applied retirement reports and manual SQL are committed on
`codex/shelly-sequence-detach`; they have not been merged into local main or
synchronized to the VPS checkout. The VPS host `api/readers/shelly_storage.py`
is absent even though the live API image contains it: host source is not proof
of the running image. Any application cleanup needs verified image builds and
a separately scoped API/ingestor rollout.

The main checkout's unrelated six dirty files were not modified. This audit
identifies a cleanup scope; it does not authorize applying it.

Machine-readable evidence: [CLEANUP-AUDIT-20261005.json](CLEANUP-AUDIT-20261005.json).
