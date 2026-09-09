# Public monitoring API

`https://telemetry.schoolheroz.com` is the canonical public monitoring origin. Source: `api/monitoring/`. It uses the existing PostgreSQL tables, shared readers in `api/readers/` and remaining query functions in `api/main.py`; measurement requests do not pass through Render or call this API over HTTP from inside the same process.

## Public contracts

| Purpose | Route |
|---|---|
| Sessions/preferences | `/auth/login`, `/auth/me`, `/auth/preferences` |
| Authorized catalog | `/catalog/schools`, `/catalog/schools/{school_id}/rooms` |
| Consumption | `/energy/consumption/history`; separate `/energy/schools/...` insights and `/energy/devices/...` telemetry |
| PV | `/energy/production/history`, `/energy/production/forecasts` (energy-only; deployed 2026-09-06; see `PV_ENERGY.md`) |
| Environment and Home | `/indoor_environment/...` |
| Thermal calculations | `/thermal-comfort/...` |

Measurement and catalog requests require a user JWT with current database role/scope/token_version. IAQ policy and pure thermal calculations contain no private measurements. The protected `/internal/auth/session` verifier lets EnergyPlus accept VPS sessions using its existing service credential. The JWT signing secret is never distributed to Render or clients.

Catalog migration: canonical `/catalog/schools` and `/catalog/schools/{school_id}/rooms` preserve the response schemas and school authorization. The old `/schools` and `/rooms?school_id=...` aliases were retired from VPS, Caddy and Render on 2026-09-07. They return 404. The separate Render `/schools/{school_id}/simulation-schedule-defaults` compute route is retained. See `../backups/catalog-interval-retirement-20260907/REPORT.md` for caller checks and rollout evidence.

Set `AUTH_TOKEN_SECRET` to a cryptographically random secret of at least 32 characters. Missing configuration fails closed. Existing user credentials and preferences stay in `app_users`; no migration is required. Public login uses the existing password verifier and rate limiter. Cross-school checks precede all shared caches.

PV history and forecasts accept inclusive Athens `start_date`, `end_date` and
`interval`, and return energy in kWh. The old `resolution` parameter and solar
routes are retired. Server-side calculation and caching preserve DST, missing
or partial coverage and measured zero. See [PV energy](PV_ENERGY.md) for the
current range, interval and cache policies.

Home environment reuses a bounded cache and eight workers and supports NDJSON streaming. Direct monitoring reads have a shared concurrency cap. Existing SQL aggregation, watermarks and precomputed hourly consumption tables remain canonical. The PV energy-only implementation moves web and native consumers to `/energy/production/history` and web forecasts to `/energy/production/forecasts`. The VPS API and web release (`c4e61f0`) are deployed and verified on 2026-09-06; iOS 1.0.0 build 4 is signed and tested, with physical-device installation skipped by user instruction. Render needs no change for these direct VPS requests. The three old `/energy/production/solar/*` routes were removed in production on 2026-09-06 and now return 404, following explicit acceptance of the impact on the old iPhone installation. Low-level `/pv/*` service contracts remain unchanged. See [PV energy contract](PV_ENERGY.md) for units, coverage, caching and rollback constraints.

## Internal latest readers

Latest UPAT and Shelly snapshots are read directly through
`api/readers/measurements.py`. Monitoring services pass Python arguments and
retain native timestamps until response serialization; they do not construct
URLs or call the history adapter for latest data. The minute-average SQL,
metric filtering, snapshot limits and response envelopes are unchanged.

`api/database.py` owns the existing connection settings and Athens session
timezone. The remaining handlers still import that factory through `main`.
`monitoring/read_limits.py` owns the same eight-slot monitoring budget and
five-second acquisition timeout shared with history reads. Existing low-level
HTTP route authentication and concurrency behavior are unchanged.

This is the first reader-extraction batch. History SQL, hourly energy,
database schemas, production image selection and collectors are unchanged.
The new modules are included by the API Dockerfile. Rollout requires a
separately selected image built from the committed source.

The first batch was activated on 8 September 2026 from commit
`7640d7e989a75c8c75aa2ac86ad1d5dc60623769`. Its immutable VPS image is
`sha256:e2ac2b61f0cf8a8b328d0539af1ec9a08a1a0ea037defd7ba9029b4d40fb9b4e`.
The build used the committed API source and the preceding image's unchanged
Python dependencies. Runtime source hashes and OpenAPI were verified. The
candidate passed 289 checks with database connections forced read-only; the
activated API passed 286 checks, including public HTTPS latest reads and ACLs.
Only API was recreated. PostgreSQL identity/start time, other containers,
Caddy and environment settings were unchanged. During the 26.67-second API
readiness window, 12 UPAT and 13 Shelly raw messages were received; this window
is not an independently measured outage duration.

Verified backup, rollback instructions and sanitized receipts are retained at
`/opt/schoolheroz-latest-reader-rollout-20260908` on the VPS. The previous image
archive was verified against all 63 OCI blobs and reused by hard link to avoid
duplicating it. No database backup/restore or migration was needed. Rollback
was not used. The installed checkout's reviewed API paths were aligned with
the committed source, preserving unrelated changes. Web and Render HTTP checks
passed; the user subsequently confirmed the authenticated consumer visual checks.
No consumer build or deployment was performed.

## Internal hourly energy reader

The second batch moves precomputed hourly Wh reads to
`api/readers/shelly_energy.py`. School energy insights call that reader directly
through their existing service, passing native timestamps and retaining the
shared eight-slot monitoring budget and five-second acquisition timeout. The
internal URL dispatch, query-string construction and intermediate JSON encoding
for hourly energy are removed. Insights accept native instants while preserving
validation and newest-row deduplication, including the repeated Athens DST hour.

The existing `/shelly/hourly-energy` service route delegates to the same reader.
Its query parameters, defaults, response, authentication and SQL are unchanged.
Device normalization and energy-window resolution are shared with the other
retained Shelly handlers. The service still normalizes its requested window to
minutes and enforces the existing 90-day maximum. No consumption calculation,
public endpoint, database schema, collector or production image pin changes.
The generic UPAT/Shelly device-history adapter remains for a separate batch.

Validation used an isolated source snapshot and disposable local PostgreSQL:
475 tests and 12 subtests passed; four optional tests were skipped. Comparison
against the preceding commit produced identical results for 36 HTTP-handler
cases, 36 service cases and two complete school-insights calculations. All 78
executed SQL statements and parameters matched, as did the OpenAPI schema
(51 paths). The existing Dockerfile already copies the entire readers package.
The second batch was activated on 8 September 2026 from commit
`96ac96cf68f4be47a83ae54a53362c40d555ee08`. Its immutable image is
`sha256:f2cc6719c8f722987265b13417c170eb74c3a5518db20e273f2ca7cbd0389089`.
The build retained every installed dependency from the preceding API image;
runtime hashes match the committed source. Candidate acceptance passed 309
checks with database connections forced read-only, followed by 300 checks on
the activated API, including public HTTPS hourly energy and school insights.
The initial acceptance script incorrectly required nonempty insights for both
schools. The previous API already reports school_22 as unavailable with zero
analyzed devices; the corrected check verifies identical populated and
unavailable results without changing the image or API code.

Only API was recreated. Readiness passed after 25.8 seconds from activation
start (not an independently measured outage duration). PostgreSQL identity and
start time, every other container and mount, Caddy and environment settings
were unchanged. During that readiness window, 14 UPAT and 23 Shelly raw messages
were persisted. Five reviewed runtime source paths were aligned in the VPS
checkout; unrelated changes were preserved. No migration or collector restart
was performed. The temporary candidate was removed; no API tracebacks or HTTP
500 responses were found after activation.

Verified source/configuration backup, the preceding image archive (all 68 OCI
blobs verified) and automatic rollback are retained root-private at
`/opt/schoolheroz-hourly-reader-rollout-20260908`. Rollback was not used.
Authenticated web checks with school_10 passed for consumption, energy insights,
environmental live/history data and PV production. Render health passed. The
iOS visual check awaits a free Simulator, which displayed another application
after SchoolHeroZ was launched. No consumer build, installation or deployment
was performed.

## Safe deployment

1. Validate and commit the scoped source in an isolated checkout. Inspect current source hashes, container IDs/start times and database health. Keep a source/config backup and tag the previous API image.
2. Build from that commit and record its mapping to the immutable image ID. Run the candidate API on a loopback-only port with the existing Docker network. Validate bounded, authenticated reads before switching it into service.
3. Recreate only API: `docker compose -f docker-compose.prod.yml up -d --no-deps --no-build api` after selecting the tested image. Never run a full `up`, `down`, volume operation or database restart for this release.
4. Validate/reload Caddy from an explicitly copied file. The existing file bind mount previously retained a stale inode; reading `/etc/caddy/Caddyfile` inside the old container did not match the current host file. Use `docker cp caddy/Caddyfile iot_caddy:/tmp/monitoring-Caddyfile`, then `caddy validate` and `caddy reload --adapter caddyfile --config /tmp/monitoring-Caddyfile` inside Caddy. Keep the host source correct for subsequent starts.
5. Verify HTTPS, login/CORS, tenant ACLs, old internal service authentication and unchanged PostgreSQL/ingestor containers. Roll back API image and source/env plus an explicitly copied previous Caddy config if acceptance fails.

On 2026-09-06 the first public-path check triggered an automatic API rollback because of the stale Caddy mount. After correcting the explicit config reload, all 18 HTTPS acceptance checks passed. PostgreSQL and ingestor containers remained unchanged throughout; no schema or telemetry data was modified. Normal login updates its existing login timestamp.

The release backup/image references and sanitized acceptance logs are retained privately on the VPS under `/root/schoolheroz-monitoring-20260906/`; the rollback image is `schoolheroz-monitoring:rollback-20260906`. Protect backups containing environment snapshots with root-only permissions. Do not copy secrets into reports or Git.

## Tests

The release validation passed 137 tests, four optional tests skipped, and nine subtests passed, including real PostgreSQL fixtures on a disposable local database. Production acceptance covered daily/hourly PV, forecasts, consumption, Home streaming/caching, environmental histories/insights, catalogs, auth and CORS. Runtime measurements are recorded in the native repository's `ios/DataFlowAudit/vps-monitoring-migration.md`.

Render release order was session compatibility `b10174b`, direct web routing `f5a4db3`, then monitoring route removal `4dac764`. iOS configurations separately name the VPS monitoring host and Render simulation host.
