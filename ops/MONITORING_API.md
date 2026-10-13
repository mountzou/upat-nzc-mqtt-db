# Public monitoring API

`https://telemetry.schoolheroz.com` is the public monitoring origin. The API
reads PostgreSQL through `api/readers/` and remaining queries in `api/main.py`;
in-process monitoring reads do not call Render or make HTTP requests to themselves.

## Public contracts

| Purpose | Routes / contract |
| --- | --- |
| Sessions/preferences | `/auth/login`, `/auth/me`, `/auth/preferences` |
| Authorized school catalog | `/catalog/schools` (API) |
| Simulation room catalog | `/catalog/schools/{school_id}/rooms` (EnergyPlus through Caddy) |
| Consumption | [Consumption history](CONSUMPTION_HISTORY.md); separate school insights and device telemetry |
| PV | [Production history and forecasts](PV_ENERGY.md), in kWh |
| Environment/Home | [Environmental history](ENVIRONMENTAL_HISTORY.md), latest readings, insights and NDJSON Home streaming |
| Notifications | [Room IAQ events and per-account read state](IAQ_NOTIFICATIONS.md) |
| Thermal calculations | `/thermal-comfort/...` |

Measurement/catalog requests require a user JWT with current database
role, school scope and token_version. Scope checks precede shared caches.
`AUTH_TOKEN_SECRET` needs at least 32 random characters; missing configuration
fails closed. Credentials/preferences remain in `app_users`; login uses the
existing password verifier and rate limiter.

IAQ policy and pure thermal calculations expose no private measurements.
`GET /indoor_environment/policy?version=2` returns
`indoor-environment-iaq-v2`, with numeric thresholds keyed by `1m`, `1h`,
`24h`, `7d`, `14d`. Clients must support this shape.
The retired `/schools` and `/rooms?school_id=...` aliases return 404;
EnergyPlus' simulation-schedule-defaults compute route remains separate.

Caddy forwards the room catalog to `nzc_energyplus:8000` on their shared Docker
network. The API activated on 2026-10-13 removes its duplicate rooms endpoint,
six room JSON files and exclusive simulation schemas.

EnergyPlus accepts VPS sessions through the protected `/internal/auth/session`
verifier. JWT signing secrets stay on the VPS. Protected backend reads use
[their separate service credential and allowlist](SERVICE_ACCESS.md).

## Internal reads

- `readers/measurements.py`: minute-average UPAT/Shelly latest snapshots,
  metric filters and snapshot limits, with native timestamps until serialization.
- `readers/shelly_energy.py`: precomputed hourly Wh for energy insights and
  `/shelly/hourly-energy`. Existing validation, newest-row deduplication, DST
  distinctions and the 90-day service-window maximum remain.
- `database.py`: connection settings and Athens session timezone.
- `monitoring/read_limits.py`: eight shared read slots and a five-second
  acquisition timeout. Canonical SQL, rollup watermarks and persisted energy
  remain the data sources.

The history adapter passes family, device ID and validated
`HistoryQueryParams` to `monitoring/local_data.read_device_history`.
URL dispatch and `DEVICE_API_BASE_URL` are removed from that local adapter;
UPAT raw/rollup selection, Shelly compact reads and SQL stay unchanged.
The API also retires `/shelly/energy`,
`/shelly/device/{device_id}/hourly-energy` and
`/shelly/device/{device_id}/energy`, with their exclusive helper/imports.
`/shelly/hourly-energy`, its protected service version and school insights remain.
The source also retires raw `/upat/devices`, `/shelly/devices`,
`/simulations/recordings/latest` and `/weather/hourly/latest` readers. The active
weather forecast/day-ahead readers and the EnergyPlus simulation proxy remain.
These cleanups were activated in the running API on 2026-10-13. The eight
retired API paths returned the expected 404 responses during acceptance.

## Deployment and recovery

Use the [production ownership guide](postgres-volume/COMPOSE-OWNERSHIP.md) and
[current recovery requirements](shelly-compact/RECOVERY.md). The API image,
activation evidence and API-only rollback are recorded in the
[2026-10-13 activation guide](api-cleanup-activation/README.md). The unchanged
Shelly ingestor mapping remains in the
[2026-10-05 rollout record](compact-only-images/ROLLOUT-20261005.md).

1. Validate and commit only the scoped source. Inspect the live baseline, build
   from that commit and record the immutable image ID and rollback pair.
2. Check a candidate with bounded authenticated reads; verify login/CORS,
   tenant ACLs, current public contracts and service-token access.
3. Use the canonical project/env/Compose and recreate only the approved service
   with `--no-deps --no-build --pull never`. An API source change does not
   require restarting PostgreSQL or ingestors.
4. For a Caddy change, validate the candidate and confirm its effective loaded
   configuration matches the intended host source. File bind mounts can retain
   a stale inode; explicitly copy/reload the reviewed candidate when needed.
5. Recheck internal health, public reads and unchanged unrelated containers.
   Recovery restores a verified compatible image/configuration pair and retains
   current storage, timestamps and consumer contracts.

Protect any environment-containing recovery material with root-only permissions.
Use existing API contract/reader tests against disposable local PostgreSQL;
fixed historical test counts and release images remain in Git history.
