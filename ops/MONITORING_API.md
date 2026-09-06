# Public monitoring API

`https://telemetry.schoolheroz.com` is the canonical public monitoring origin. Source: `api/monitoring/`. It uses the existing PostgreSQL tables and local query functions in `api/main.py`; measurement requests do not pass through Render or call this API over HTTP from inside the same process.

## Public contracts

| Purpose | Route |
|---|---|
| Sessions/preferences | `/auth/login`, `/auth/me`, `/auth/preferences` |
| Authorized catalog | `/schools`, `/rooms?school_id=...` |
| Consumption | `/energy/schools/...`, `/energy/devices/...` |
| PV | `/energy/production/solar/aggregates`, `/energy/production/solar/day-ahead-forecasts` |
| Environment and Home | `/indoor_environment/...` |
| Thermal calculations | `/thermal-comfort/...` |

Measurement and catalog requests require a user JWT with current database role/scope/token_version. IAQ policy and pure thermal calculations contain no private measurements. The protected `/internal/auth/session` verifier lets EnergyPlus accept VPS sessions using its existing service credential. The JWT signing secret is never distributed to Render or clients.

Set `AUTH_TOKEN_SECRET` to a cryptographically random secret of at least 32 characters. Missing configuration fails closed. Existing user credentials and preferences stay in `app_users`; no migration is required. Public login uses the existing password verifier and rate limiter. Cross-school checks precede all shared caches.

PV aggregation accepts inclusive Athens `start_date`, `end_date` and required `resolution=day|hour`, with a maximum of 90 days. SQL groups by day for Home and UTC hour plus day for detail. Daily requests do not read hourly buckets. DST, missing/partial coverage and observed zero are preserved. SQL integrates five-minute kW samples into kWh; raw samples never leave PostgreSQL on this path. The bounded per-process cache stores 180 prepared dates for five minutes and coalesces concurrent overlapping requests.

Home environment reuses a bounded cache and eight workers and supports NDJSON streaming. Direct monitoring reads have a shared concurrency cap. Existing SQL aggregation, watermarks and precomputed hourly consumption tables remain canonical. The legacy web PV chart still uses `/energy/production/solar/active-power` on VPS; native views exclusively use aggregates.

## Safe deployment

1. Inspect current source hashes, container IDs/start times and database health. Keep a source/config backup and tag the previous API image.
2. Build and run the candidate API on a loopback-only port with the existing Docker network. Validate bounded, authenticated reads before switching it into service.
3. Recreate only API: `docker compose -f docker-compose.prod.yml up -d --no-deps --no-build api` after selecting the tested image. Never run a full `up`, `down`, volume operation or database restart for this release.
4. Validate/reload Caddy from an explicitly copied file. The existing file bind mount previously retained a stale inode; reading `/etc/caddy/Caddyfile` inside the old container did not match the current host file. Use `docker cp caddy/Caddyfile iot_caddy:/tmp/monitoring-Caddyfile`, then `caddy validate` and `caddy reload --adapter caddyfile --config /tmp/monitoring-Caddyfile` inside Caddy. Keep the host source correct for subsequent starts.
5. Verify HTTPS, login/CORS, tenant ACLs, old internal service authentication and unchanged PostgreSQL/ingestor containers. Roll back API image and source/env plus an explicitly copied previous Caddy config if acceptance fails.

On 2026-09-06 the first public-path check triggered an automatic API rollback because of the stale Caddy mount. After correcting the explicit config reload, all 18 HTTPS acceptance checks passed. PostgreSQL and ingestor containers remained unchanged throughout; no schema or telemetry data was modified. Normal login updates its existing login timestamp.

The release backup/image references and sanitized acceptance logs are retained privately on the VPS under `/root/schoolheroz-monitoring-20260906/`; the rollback image is `schoolheroz-monitoring:rollback-20260906`. Protect backups containing environment snapshots with root-only permissions. Do not copy secrets into reports or Git.

## Tests

The release validation passed 137 tests, four optional tests skipped, and nine subtests passed, including real PostgreSQL fixtures on a disposable local database. Production acceptance covered daily/hourly PV, forecasts, consumption, Home streaming/caching, environmental histories/insights, catalogs, auth and CORS. Runtime measurements are recorded in the native repository's `ios/DataFlowAudit/vps-monitoring-migration.md`.

Render release order was session compatibility `b10174b`, direct web routing `f5a4db3`, then monitoring route removal `4dac764`. iOS configurations separately name the VPS monitoring host and Render simulation host.
