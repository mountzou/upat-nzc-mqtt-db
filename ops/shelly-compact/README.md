# Shelly compact storage: current and historical files

Verified on 2026-09-29: the production shelly_ingestor writes compact-only
(SHELLY_MEASUREMENTS_WRITE_MODE=compact), while iot_api reads
shelly_compact.readings with one-decimal averaging. The active Python files
api/main.py, api/readers/measurements.py, api/readers/shelly_storage.py,
shelly-ingestor/main.py and shelly-ingestor/measurements.py match the running
images byte-for-byte by SHA-256. The existing shelly_compact.series and
measurements tables and readings view were observed using a read-only catalog
query. The old public.shelly_measurements table still exists.

PRODUCTION-STAGE3-20260916.md and VALIDATION-20260916.md record the completed
cutover. The staged Compose files, stage1.py, stage2.py, stage3.py,
writer-cutover.py and migrate.py are historical migration/recovery tools.
Their embedded image identities, checkpoints and commands are dated evidence,
not a current rollout or rollback procedure. Do not rerun them against the
live database without a fresh design and authorization. In particular, do not
remove the legacy table or change its sequence ownership from this checkout.

The current deployment contract is ../../docker-compose.prod.yml; PostgreSQL
ownership and deployment boundaries are documented in
../postgres-volume/COMPOSE-OWNERSHIP.md.
