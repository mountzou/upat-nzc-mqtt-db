# Compact-only production-runtime images

The active API/Shelly image, source and configuration mapping is kept in
[the rollout record](ROLLOUT-20261005.md), with the retained VPS recovery location.
See [current recovery requirements](../shelly-compact/RECOVERY.md).

## Build scope

These Dockerfiles overlay specific source files on an explicitly selected
`BASE_IMAGE`. Python, packages, command and entrypoint are inherited; no pip
installation or dependency change occurs. Verify the base archive/configuration
against the inspected image, build from the repository root for linux/amd64,
and label the candidate with its committed Git revision.

The API recipe copies only `api/readers/shelly_storage.py`; the ingestor recipe
copies `main.py` and `measurements.py`. Review every intended changed runtime
file before a future build. The pending broader API cleanup needs an updated,
verified build scope; these narrow overlays alone do not include it.

## Deployment boundary

Use [canonical production ownership](../postgres-volume/COMPOSE-OWNERSHIP.md)
and target only the approved application services. Preserve PostgreSQL, Caddy
and unrelated services. Bootstrap SQL is for fresh databases; image updates
do not require replaying init.sql or migration 014 on the existing compact schema.
