# Protected backend data access

Status: **deployed and verified on 2026-09-07**. Render now uses the authenticated HTTPS service. Host port 8000 is bound only to `127.0.0.1`; an external connection test returned connection refused. See `backups/service-access-rollout-20260907/REPORT.md`. The caller audit below records the pre-migration state.

## Verified callers and evidence boundary

The running Render service `upat-nzc-energyplus-backend` (`srv-d6nb13k50q8c73b2bri0`, instance `hvh6p`) reports commit `fda71006a6396176d96f487b60de992f86df95fd`. Importing its deployed `app.config` in the authenticated Render web shell confirmed `DEVICE_API_BASE_URL=http://65.21.106.41:8000`, with no environment override. A read-only request from that instance to `${DEVICE_API_BASE_URL}/health` returned HTTP 200 with the database connected.

The following paths are reachable from that exact deployed source. This is a source call graph plus verified runtime origin, not a claim that every business operation was exercised during the observation window.

| Existing GET path | Render caller and purpose |
| --- | --- |
| `/health` | `operations_summary.py`: device API health; separate `httpx.get`, outside shared client |
| `/upat/device/{device_id}/latest` | `service_device_api.py`: current environmental data, overview |
| `/upat/device/{device_id}/history` | `service_device_api.py`: environmental history/research |
| `/shelly/hourly-energy` | `service_energy_api.py`: `research_exports/energy_consumption.py` |
| `/weather/hourly/forecast` | `service_weather_forecast.py`: EPW/simulation weather |
| `/simulations/day-ahead/latest` | `service_pred_demand.py`: predicted demand/planning |
| `/pv/day-ahead/latest` | `service_pred_production.py`: existing backend PV latest reader |

Existing PV actuals, PV range forecasts and operations telemetry already use HTTPS and their existing service authentication. Keep those contracts and credentials separate. Raw Shelly latest/history helper definitions were found, but no reachable deployed caller was identified; they are excluded from this new allowlist. This preparation preserves the research reader's existing calculation contract and does not reintroduce the retired public consumption endpoints.

VPS collectors, hourly aggregation and PostgreSQL maintenance use the database directly. Root cron references `weather-collector`, `pv-prediction` and `simulation-recorder`; the minute-by-minute dispatchers have their own scheduling conditions. The simulation recorder calls Render and therefore has a transitive dependency on Render's VPS data/weather access. Caddy uses the private Docker upstream `api:8000`. Preserve loopback operational checks until their migration is verified. No provider or writer job was triggered for this audit.

The bounded access-log sample includes previous audit/test traffic. It cannot identify every external caller or prove that an unused path has no callers. Future port closure requires successful consumer migration and a new observation window.

## Prepared contract

For each path above, add the prefix `/internal/data`, for example:

```http
GET /internal/data/upat/device/portable-108/history?start=2026-09-05T08:00:00Z&end=2026-09-05T09:00:00Z&interval=1h
Authorization: Bearer <server-only DATA_SERVICE_TOKEN>
```

The implementation reuses the original FastAPI handlers and preserves their parameters, validation and responses. It adds no duplicate SQL or energy calculation. In particular, weather bounds retain the date format sent by the deployed weather caller; this patch does not normalize unrelated time contracts.

- Only the seven reviewed GET routes are registered. No generic forwarding or write methods.
- Caddy forwards these specific paths over the existing HTTPS host. API authentication is enforced even when Caddy is bypassed.
- `DATA_SERVICE_TOKEN` is an independent, randomly generated server credential with at least 32 characters. It is never a browser/iOS setting or a user JWT signing secret.
- Missing or too-short configuration returns 503. Missing, wrong or duplicate Authorization credentials return 401. Query-string credentials are not accepted. Authentication failures use `Cache-Control: no-store`.
- A service credential does not authorize a public user route. Trusted backend access is cross-school; Render must keep its existing user authorization before calling these readers.
- Both Compose configurations pass the optional environment variable to the API. The blank example intentionally fails closed until a secret is provisioned.

## Verification

Local focused suite: **140 passed, 8 skipped**. The skipped tests require a disposable PostgreSQL fixture. Separate isolated candidate checks used read-only PostgreSQL sessions against existing data: **87 service/proxy checks** and **132 retained-contract checks**. All seven protected readers returned HTTP 200 and matched production responses in that test window. Original Caddyfile validation passed.

The candidate was layered on the exact active production API image, replacing only `main.py` with its minimal registration addition and adding `monitoring/service_access.py`. It was not built from the broader dirty local checkout. Runtime inventory comparison confirmed this scope. Temporary API/Caddy containers had no host ports; their database connections were read-only. Production service identities/start times/restart counts, API sources, active Caddy configuration and protected checkout files stayed unchanged. Temporary containers and the ephemeral test credential were removed.

Evidence: `backups/service-access-preparation-20260907/REPORT.md` and its JSON receipts. Current prepared image: `schoolheroz-service-access-candidate:20260907`, immutable ID `sha256:5c8e7fdedb81de0087da5b0362f52e1a5a278338e2e1348ba7a49c70770aa4b1`.

## Rollout sequence (completed)

1. Recheck live source/image/configuration against the candidate manifest and take a fresh narrow backup. Provision a new production `DATA_SERVICE_TOKEN` securely. Activate the tested API addition and Caddy allowlist while retaining current port access. Verify missing/wrong-token rejection and authenticated reads over production HTTPS. Avoid deploying the dirty checkout wholesale.
2. Update Render's shared device HTTP client to send the credential only to the configured trusted HTTPS origin, without credential-bearing redirects. Migrate the separate operations health client too. Set `DEVICE_API_BASE_URL=https://telemetry.schoolheroz.com/internal/data` and the same production credential. Keep authorization at the backend boundary. Test overview/history, research export, weather/simulation inputs, predictions and health. Recheck direct web/iOS origins and any operator consumers before closure.
3. After observing the migrated deployment and relevant scheduled workflows, restrict the host publication of port 8000 to loopback, retaining Docker `api:8000` for Caddy. Verify from outside the VPS that direct access fails and HTTPS remains healthy. No database migration or MQTT stop is required by this design.

Before step 3, Render can be rolled back to its previous deployment/origin while the old port remains available. After restriction, rollback must restore the verified previous port publication before reverting a consumer that still requires it. Remove the old low-level handlers only after their remaining internal dependencies are separately refactored; closing the external port does not require deleting working database readers.

Preparation did not change production. The subsequently authorized rollout provisioned a production credential, activated API/Caddy, deployed Render commit `3d81b89d6c07dbf76cb63170cf151c21d848778f`, and restricted port 8000. Render uses the new HTTPS default from code; `DEVICE_API_BASE_URL` remains without an environment override. All seven readers and operations health were verified from instance `cq9jz` both before and after restriction.

The first activation exposed a stale Caddy bind mount and automatically rolled the API back. The original active Caddy configuration was restored with an exact hash check. Recreating only Caddy with the same image and persistent volumes corrected the mount; the second activation passed. Collectors, PostgreSQL and MQTT were not restarted.

Emergency port-only rollback: `python3 /opt/schoolheroz-service-access-rollout-20260907/restore_public_port.py` (reopens external HTTP; use only when rollback is intended). This retains the protected API image and credential. Do not run the earlier full API rollback while the migrated Render deployment is active: that would remove its new upstream routes. Restore consumer compatibility first. Full backups and the exercised initial API rollback remain in the same protected directory.
