# Internal authentication service contract

This boundary lets the Render backend use application users stored in the VPS
PostgreSQL instance without exposing PostgreSQL to Render or returning password
hashes over the network.

The endpoints are server-to-server contracts. They require a dedicated bearer
secret from `AUTH_SERVICE_TOKEN`; the operational telemetry token and the Render
JWT signing secret must remain separate. When the service token is absent or
shorter than 32 characters, these endpoints fail closed with HTTP 503.

## Endpoints

`POST /internal/auth/verify` accepts `username` and `password`. It returns only
the active user's public identity, role, school scope, `token_version`, theme,
and completed onboarding keys after the password has been verified inside the
VPS API. A successful verification records `last_login_at` before the response
is returned. Missing users, inactive users, and incorrect passwords share the same
HTTP 401 response and never change the login timestamp.

`POST /internal/auth/resolve` accepts `username`. It returns the same public
identity for an active user and lets the Render backend compare current role,
scope, and `token_version` with a signed JWT. Missing and inactive identities
share the same HTTP 404 response.

`POST /internal/auth/session` accepts a VPS-issued `access_token`. It verifies
its signature, expiry, issuer and audience, then checks the active user's
current database role, school scope and `token_version`. It returns the public
identity and `token_version`; invalid, expired or revoked sessions return HTTP
401, and unavailable authentication returns HTTP 503. It uses the same
`AUTH_SERVICE_TOKEN` bearer credential as the other internal auth endpoints.
EnergyPlus can therefore validate VPS sessions without receiving the VPS JWT
signing secret.

`PATCH /internal/auth/preferences` accepts `username` plus a light/dark theme,
one versioned onboarding completion change, or both. It updates only an active
identity and returns the current public identity. The Render backend derives
the username from its validated JWT; browsers never call this internal route
or receive the service bearer token.

All endpoints set `Cache-Control: no-store`. No endpoint returns
`password_hash`, creates users, changes credentials/roles/scopes, or issues
application JWTs.
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
read-only `/resolve` and `/session` endpoints are not part of the
password-attempt budget.

## Transport and trust boundary

The Render caller uses the HTTPS Caddy route on `telemetry.schoolheroz.com`;
it must never send credentials to the VPS IP or port 8000 over plaintext HTTP.
Production publishes the API port only as `127.0.0.1:8000:8000`. Caddy reaches
`api:8000` over the Docker network. The loopback binding and auth allowlist were
rechecked on 5 October 2026.

Caddy forwards only `POST /internal/auth/verify`,
`POST /internal/auth/resolve`, `POST /internal/auth/session`, and
`PATCH /internal/auth/preferences`; other methods and paths under
`/internal/auth/` remain outside the reverse-proxy allowlist.

`AUTH_SERVICE_TOKEN` must be generated as a high-entropy secret, stored only in
the VPS and Render secret stores, and never reused as `OPS_TELEMETRY_TOKEN` or
`AUTH_TOKEN_SECRET`.

## Deployment history

Migrations 011 and 012 belong to the initial authentication and user-preferences
implementation. The original rollout checklist is retained in Git history.

Backend data readers migrated to authenticated HTTPS and the host port was
restricted to loopback on 7 September 2026. See
[Protected backend data access](../ops/SERVICE_ACCESS.md) for the current access contract.
Current deployments must retain the loopback binding, Caddy allowlist and
separate auth/data service credentials. Port closure is no longer a pending
rollout step.
