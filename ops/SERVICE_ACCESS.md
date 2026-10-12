# Protected backend data access

Trusted backends use `https://telemetry.schoolheroz.com/internal/data`.
Host port 8000 is bound to loopback; Caddy reaches `api:8000` on the Docker
network. Browser/iOS monitoring requests use the separate public user-JWT API.

## Read allowlist

All paths below are GET routes under `/internal/data`:

| Path | Purpose |
| --- | --- |
| `/health` | Backend operations health |
| `/upat/device/{device_id}/latest` | Latest environmental readings |
| `/upat/device/{device_id}/history` | Environmental history/research |
| `/shelly/hourly-energy` | Hourly energy research/export |
| `/weather/hourly/forecast` | Weather inputs for simulation |
| `/simulations/day-ahead/latest` | Stored demand predictions |
| `/pv/day-ahead/latest` | Stored PV predictions |

The router reuses existing handlers and preserves their parameters, validation
and responses. There is no generic forwarding or write method. Weather bounds
retain their own date contract. Other protected PV/operations readers keep
their existing credentials. Raw Shelly latest/history is outside this allowlist.

```http
GET /internal/data/upat/device/portable-108/history?start=2026-09-05T08:00:00Z&end=2026-09-05T09:00:00Z&interval=1h
Authorization: Bearer <server-only DATA_SERVICE_TOKEN>
```

## Authentication and routing

`DATA_SERVICE_TOKEN` is an independent server credential of at least 32 random
characters. Keep it in server secret stores, separate from auth/operations
credentials and the user JWT signing secret. Compose passes it to the API;
blank/short configuration returns 503.

Missing, wrong or duplicate Authorization credentials return 401, with
`Cache-Control: no-store`. Query-string credentials are not accepted.
Authentication applies inside FastAPI as well as through Caddy.

Backend clients send credentials only to the configured trusted HTTPS origin,
without credential-bearing redirects. This service access is cross-school:
the calling backend must enforce its user authorization first. A service token
does not authorize public user routes. Collectors and maintenance jobs keep
their direct database access.

## Verification and recovery

Check authenticated reads, missing/wrong-token rejection, backend workflows,
loopback health and public HTTPS separately. The public Caddy health path is
not the loopback/service health endpoint. Preserve the loopback publication,
Caddy allowlist and service credential when updating API or backend code.

Recovery must retain the routes and contracts required by the current backend.
Select the matching verified API/configuration pair described by
[current recovery requirements](shelly-compact/RECOVERY.md).
An older API without these routes cannot serve the migrated backend. Changing
consumer origin or reopening a host port is a separate reviewed network change,
not an automatic application rollback.

Historical caller audits, candidate images and installation steps remain in
Git history. The current source contract is `api/monitoring/service_access.py`.
