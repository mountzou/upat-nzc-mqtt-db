# Shelly compact storage: migration history

Current contract, 2026-10-05: API reads and ingestor writes are fixed to compact.
The retired storage/write/rounding selectors are no longer part of the runtime.
Shelly averages retain one-decimal numeric rounding; raw measurements are unchanged.
Current recovery boundaries are in [RECOVERY.md](RECOVERY.md).

Legacy retirement, 2026-10-05: `public.shelly_measurements` and its own indexes
were removed after full history/dependency verification. Compact remains active;
the existing public ID sequence is independent. About 14.54 GB was reclaimed.
See [the applied retirement and verification](LEGACY-RETIRED-20261005.md).

Sequence update, 2026-10-05: `public.shelly_measurements_id_seq` is now
free-standing (`OWNED BY NONE`), preserving its name and current value.
The legacy table was retained at that step. See [the applied change and verification](SEQUENCE-DETACHED-20261005.md).

Historical observation, 2026-09-29: the production shelly_ingestor writes compact-only
(SHELLY_MEASUREMENTS_WRITE_MODE=compact), while iot_api reads
shelly_compact.readings with one-decimal averaging. The active Python files
api/main.py, api/readers/measurements.py, api/readers/shelly_storage.py,
mqtt/shelly-devices/main.py and mqtt/shelly-devices/measurements.py match the running
images byte-for-byte by SHA-256. The existing shelly_compact.series and
measurements tables and readings view were observed using a read-only catalog
query. At that checkpoint the old public.shelly_measurements table still existed.

PRODUCTION-STAGE3-20260916.md and VALIDATION-20260916.md record the completed
cutover. This directory contains dated evidence and current recovery guidance. The staged
controllers (`stage1.py`, `stage2.py`, `stage3.py`), migration/recovery helpers
(`copy-window.py`, `shadow-api.py`, `migrate.py`, `writer-cutover.py`) and
`compose.stage1.prod.yml`, `compose.stage2.prod.yml`, `compose.stage3.prod.yml`
have been retired from the repository. Their source remains in Git history.
The recorded image identities, checkpoints and commands are dated evidence.
The one-off cleanup controller is bound to the reviewed artifact inventory and
only retires files; it does not migrate data or restart services.
The September reverse-copy recovery is obsolete after legacy-table retirement.
Restoring legacy/dual modes or the old UTC-naive timestamp contract cannot serve
as recovery for today's deployment. Application rollback must preserve compact
storage and use a compatible image/configuration pair. The original helpers
remain in Git history; they are not current operating procedures.

`compose.override.yml`, `smoke-local-images.py`, `build-local.sh`,
`Dockerfile.api` and `Dockerfile.ingestor` have been retired from this directory.
They remain available in Git history as build and rehearsal artifacts for the
completed migration.

The current deployment contract is ../../docker-compose.prod.yml; PostgreSQL
ownership and deployment boundaries are documented in
../postgres-volume/COMPOSE-OWNERSHIP.md.
