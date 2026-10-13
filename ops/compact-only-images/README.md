# Compact-only production-runtime images

The [2026-10-05 rollout record](ROLLOUT-20261005.md) records the compact-only
API/Shelly overlays and their retained VPS recovery location. Its Shelly ingestor
image remains the deployed version; its API image is the retained rollback image
for the [API cleanup activated on 2026-10-13](../api-cleanup-activation/README.md).
Use that newer guide for the active API mapping and API-only rollback.
See [current recovery requirements](../shelly-compact/RECOVERY.md).

## Build scope

These Dockerfiles overlay specific source files on an explicitly selected
`BASE_IMAGE`. Python, packages, command and entrypoint are inherited; no pip
installation or dependency change occurs. Verify the base archive/configuration
against the inspected image, build from the repository root for linux/amd64,
and label the candidate with its committed Git revision.

The API recipe copies only `api/readers/shelly_storage.py`; the ingestor recipe
copies `main.py` and `measurements.py`. Review every intended changed runtime
file before a future build. The broader API cleanup used the separate
[complete-source recipe](../api-cleanup-activation/README.md); these narrow
overlays alone do not reproduce the API activated on 2026-10-13.

## Deployment boundary

Use [canonical production ownership](../postgres-volume/COMPOSE-OWNERSHIP.md)
and target only the approved application services. Preserve PostgreSQL, Caddy
and unrelated services. Bootstrap SQL is for fresh databases; image updates
do not require replaying init.sql or migration 014 on the existing compact schema.
