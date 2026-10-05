# Shelly compact storage: migration history

Sequence update, 2026-10-05: `public.shelly_measurements_id_seq` is now
free-standing (`OWNED BY NONE`), preserving its name and current value.
The legacy table remains. See [the applied change and verification](SEQUENCE-DETACHED-20261005.md).

Verified on 2026-09-29: the production shelly_ingestor writes compact-only
(SHELLY_MEASUREMENTS_WRITE_MODE=compact), while iot_api reads
shelly_compact.readings with one-decimal averaging. The active Python files
api/main.py, api/readers/measurements.py, api/readers/shelly_storage.py,
mqtt/shelly-devices/main.py and mqtt/shelly-devices/measurements.py match the running
images byte-for-byte by SHA-256. The existing shelly_compact.series and
measurements tables and readings view were observed using a read-only catalog
query. The old public.shelly_measurements table still exists.

PRODUCTION-STAGE3-20260916.md and VALIDATION-20260916.md record the completed
cutover. This directory now contains historical documentation only. The staged
controllers (`stage1.py`, `stage2.py`, `stage3.py`), migration/recovery helpers
(`copy-window.py`, `shadow-api.py`, `migrate.py`, `writer-cutover.py`) and
`compose.stage1.prod.yml`, `compose.stage2.prod.yml`, `compose.stage3.prod.yml`
have been retired from the repository. Their source remains in Git history.
The recorded image identities, checkpoints and commands are dated evidence.
Any future rollout or rollback needs a fresh design and authorization. In
particular, do not remove the legacy table or change its sequence ownership
from this checkout.

`compose.override.yml`, `smoke-local-images.py`, `build-local.sh`,
`Dockerfile.api` and `Dockerfile.ingestor` have been retired from this directory.
They remain available in Git history as build and rehearsal artifacts for the
completed migration.

The current deployment contract is ../../docker-compose.prod.yml; PostgreSQL
ownership and deployment boundaries are documented in
../postgres-volume/COMPOSE-OWNERSHIP.md.
