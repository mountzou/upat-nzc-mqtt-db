# API cleanup activation, 2026-10-13

This recipe activates reviewed application source `1cd68d9246782e64779f44031b1d6fb51b01b7d3` while retaining the exact Python 3.12.14 and installed packages of the current amd64 API. It removes obsolete catalogs rather than leaving them behind in an overlay.

Baseline: `schoolheroz-api:compact-only-overlay-0ac942b9b89e`, immutable image `sha256:c1d1970522a08e90b571313146da7ce9b725e3341120ad424f35bdfc25c7218a`.

## Build and acceptance

1. Run `export.py --repo <checkout> --destination <new-context>` to export the 60 exact Git blobs. Record the recipe commit separately from the application commit. Use a fresh private staging directory outside the production checkout.
2. Verify the local baseline reference resolves to the immutable ID above. Build with `--network=none --pull=false`, `BASE_IMAGE` set to that verified reference, the pinned `SOURCE_REVISION`, and the exact `RECIPE_REVISION`. Use a unique `schoolheroz-api:cleanup-1cd68d9-<recipe-commit>` tag. Do not use the generic API Dockerfile or install/upgrade runtime packages.
3. Check every runtime source against `source-manifest.json`, the absence of eight obsolete JSON files, and equality of Python/package fingerprints, CMD, entrypoint, user, working directory and runtime image settings. The image must serve 45 OpenAPI paths; all retained route definitions and common public schemas must match the baseline's 53-path schema.
4. Run the API test suite against the actual image. Test dependencies are separate read-only mounts, never a runtime image layer. Supply only fixture credentials and a disposable PostgreSQL container, with no production data, volumes, credentials or network. Enable the PostgreSQL integration suites explicitly. Skips/failures require an explanation before acceptance.
5. Run an isolated canary on a verified free loopback port, with no production aliases or Caddy traffic. Rehearse the old image in the same disposable environment, checking the original source/packages and 53 paths. Production remains untouched until candidate acceptance.

## Production switch

Select a quiet window, without an active IAQ job and with sufficient time before the next scheduled run. Preserve the production Compose symlink, Caddy file and loaded configuration, environment values, PostgreSQL ownership/data and every other container/timer. Recreating the API can briefly interrupt requests and streams.

`switch_api.py prepare --backup /var/backups/upat-nzc-mqtt-db/<new-release> --candidate <tag>` creates a root-only directory with the exact previous Compose, env and Caddy files, full private API inspection, protected-state fingerprints and verified baseline image archive. Its configuration guard permits only `/services/api/image` to change in the complete resolved Compose model. Never publish the private backup, resolved configuration, tokens or container environment.

After candidate acceptance, run `switch_api.py activate --backup <same-directory>`. The only service operation is:

```sh
docker compose --project-directory /opt/upat-nzc-mqtt-db \
  --env-file /opt/upat-nzc-mqtt-db/.env -p upat-nzc-mqtt-db \
  -f /opt/upat-nzc-mqtt-db/docker-compose.prod.yml \
  up -d --no-deps --no-build --pull never api
```

The installed PostgreSQL/Compose validator checks the current configuration before preparation and after activation. Its historical `--baseline` option is not a generic deployment comparison and must not be used here.

Verify exact image/source/packages, retained route/schema contracts, bounded health/authenticated read requests, tenant scope/service allowlists, environmental/energy history, Home streaming, notification listing, CORS and the EnergyPlus room catalog. Public `/health` belongs to EnergyPlus; use the API loopback for its health check. Avoid login (updates last login), preference updates, notification mark-read and manual job execution in production. Observe errors/latency for 10–15 minutes, then verify the next normal IAQ hourly run and the daily run when it occurs; distinguish pending scheduled evidence from verified success.

## Rollback

Rollback immediately for a failed 120-second startup, unexpected contract/auth/scope/CORS/data differences, repeated new 5xx/tracebacks or material latency regression. Expected 404s on retired API-only paths are not failures.

Run `switch_api.py rollback --backup <same-directory>`. It refuses concurrent Compose/protected-state drift, verifies both image references and restores the exact original Compose bytes before the same API-only service operation. It checks the 53-path API, installed validator and protected containers/configuration. Perform the same bounded authenticated read checks. If a drift guard fires, investigate the precise concurrent change before any manual overwrite. If the old image was removed, first verify the saved archive SHA against the private record, then load only that archive.

No SQL, database restore, inverse migration, Caddy reload, timer changes, broad Compose operations or pruning are part of this rollout. Preserve backup and release evidence. Remove only explicitly named disposable test resources after recording their ownership and results. Keep source commit → recipe commit → immutable image → activation/rollback mapping in the release record.

Guard tests: `python3 -B -m unittest discover -s ops/api-cleanup-activation -p 'test_*.py'`.
