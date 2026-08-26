# Internal authentication service contract

This boundary lets the Render backend use application users stored in the VPS
PostgreSQL instance without exposing PostgreSQL to Render or returning password
hashes over the network.

The endpoints are server-to-server contracts. They require a dedicated bearer
secret from `AUTH_SERVICE_TOKEN`; the operational telemetry token and the Render
JWT signing secret must remain separate. When the service token is absent or
shorter than 32 characters, both endpoints fail closed with HTTP 503.

## Endpoints

`POST /internal/auth/verify` accepts `username` and `password`. It returns only
the active user's public identity, role, school scope, and `token_version` after
the password has been verified inside the VPS API. Missing users, inactive
users, and incorrect passwords share the same HTTP 401 response.

`POST /internal/auth/resolve` accepts `username`. It returns the same public
identity for an active user and lets the Render backend compare current role,
scope, and `token_version` with a signed JWT. Missing and inactive identities
share the same HTTP 404 response.

Both endpoints set `Cache-Control: no-store`. Neither endpoint returns
`password_hash`, creates users, changes users, or issues application JWTs.
Malformed authentication requests return a generic HTTP 422 response without
echoing submitted field values.

## Login attempt throttling

`POST /internal/auth/verify` applies a rolling 60-second limit after the caller
has passed the service-bearer check and before any database lookup or password
hash work. At most five attempts are accepted for one normalized username and
at most 60 attempts are accepted globally in that window. Rejected attempts
return HTTP 429 with `Cache-Control: no-store` and a bounded `Retry-After`
header. Usernames are represented only by an in-memory SHA-256 key in the
limiter.

The limiter is intentionally process-local because the current production API
runs one Uvicorn process. A future multi-process or multi-instance deployment
must move this state to a shared rate-limit store before scaling out. The
read-only `/resolve` endpoint is not part of the password-attempt budget.

## Transport and trust boundary

The Render caller must use the HTTPS Caddy route on
`telemetry.schoolheroz.com`; it must never send credentials to the VPS IP or
port 8000 over plaintext HTTP. The current production API still publishes port
8000 for legacy consumers, so closing that binding remains a required hardening
step before we describe this boundary as network-private.

Caddy forwards only `POST /internal/auth/verify` and
`POST /internal/auth/resolve`; other methods and paths under `/internal/auth/`
remain outside the reverse-proxy allowlist.

`AUTH_SERVICE_TOKEN` must be generated as a high-entropy secret, stored only in
the VPS and Render secret stores, and never reused as `OPS_TELEMETRY_TOKEN` or
`AUTH_TOKEN_SECRET`.

## Staged rollout

1. Validate migration `011_app_users.sql` against a disposable PostgreSQL 15
   database.
2. Apply the migration to production in a separately approved database batch.
3. Import the existing user records in a separately reviewed transaction; no
   plaintext passwords are involved.
4. Configure the same service token in VPS and Render secret stores.
5. Deploy and verify the VPS API/Caddy surface before changing Render login.
6. Add the Render repository adapter behind an explicit configuration flag,
   validate login and JWT revocation, then remove the static fallback registry
   in a later batch.

Login rate limiting is implemented in the API boundary. Removal of the public
API port remains a rollout prerequisite, but it must follow migration of the
existing EnergyPlus `DEVICE_API_BASE_URL` caller to verified HTTPS routes; an
immediate removal would break its device, weather, PV, and simulation reads.
