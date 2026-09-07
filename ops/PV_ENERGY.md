# PV production energy contract

Status: VPS API and web consumers deployed and verified on 2026-09-06. Web commit `c4e61f0` is live at `www.schoolheroz.com`. iOS 1.0.0 build 4 is built, signed and tested; physical-device installation remains skipped by user instruction. This change required no database migration or collection outage. See [VPS rollout evidence](../backups/pv-energy-rollout-20260906/REPORT.md) and [consumer rollout evidence](../backups/pv-energy-consumer-rollout-20260906/REPORT.md).

Legacy retirement: completed in production on 2026-09-06 with image `schoolheroz-pv-retired-api:20260906`. The three old app-facing routes and their orphaned aggregate service are removed; all three paths return 404. The user explicitly accepted that the old physical iPhone installation may need build 4 before its PV views work. Simulator and authenticated web validation passed. See [retirement evidence and rollback](../backups/pv-legacy-retirement-20260906/REPORT.md).

## Endpoints

Both endpoints use the existing monitoring Bearer JWT and shared `upat-pv` installation access policy.

```http
GET /energy/production/history?start_date=2026-09-01&end_date=2026-09-05&interval=1h
GET /energy/production/forecasts?start_date=2026-09-07&end_date=2026-09-07&interval=1h
GET /energy/production/forecasts?latest=true&interval=1h
```

| Parameter | Meaning |
| --- | --- |
| `start_date`, `end_date` | Inclusive calendar dates in Europe/Athens; both required for a dated request. Maximum 90 days. |
| `interval` | Defaults to `1h`. History accepts multiples of `5m`; forecasts accept multiples of `1h`. Maximum fixed duration `168h`. Both accept `day` for an Athens calendar day. The shared parser normalizes `60m` to `1h`. |
| `latest=true` | Forecasts only: selects the target date of the latest successful stored forecast run. Cannot be combined with dates. Used by the existing Home forecast cards. |

There is no `metric`, `aggregate`, `resolution`, `timezone` or legacy bucket query parameter on these new routes. Unknown or duplicated parameters and unsupported intervals return 422. No successful stored forecast for `latest=true` returns 404. A dated range without data returns an explicit missing series. Source query failure returns 503; detected invalid or duplicate source intervals return 502.

## Energy and time semantics

- History integrates canonical five-minute observations from `pv_plant_readings_5m`: each valid sample contributes `active_power_kw × (5 / 60)` kWh. Forecasts integrate the latest successful stored run for each target date: each hourly prediction contributes `predicted_power_kw × 1` kWh. All requested buckets sum those contributions on the server.
- The API returns no power fields. An hourly value of 6 kWh means 6 kWh produced during that hour; a daily value is the sum over that Athens day. Selecting a coarser interval preserves the total energy for the same date window.
- Fixed intervals use the shared UTC bin origin (`2001-01-01T00:00:00Z`). The first and last bins are clipped to the requested Athens date boundaries. Their actual `start` and `end` are authoritative; a fixed `2h` request can therefore have one-hour edge bins. Fixed `24h` is distinct from `day`.
- `day` respects Athens midnight and the 23/25-hour daylight-saving days. Response timestamps are offset-aware UTC instants; `timezone: "Europe/Athens"` is response metadata, not a selectable query option. Consumers format those instants in Athens.
- History timestamps are already `TIMESTAMPTZ`. Forecast persistence still has an explicit Athens local-civil `TIMESTAMP` contract, verified read-only against live column metadata. The query interprets that column once at the source boundary. A nonexistent spring clock hour is excluded by a round-trip check. The legacy 24-row autumn forecast cannot represent both repeated hours: the missing hour remains missing and the day is partial. No value is invented and no forecast schema migration is included here.

## Response

Common top-level fields: `source: "postgres"`, `kind: "history" | "forecast"`, `unit: "kWh"`, `timezone`, normalized `interval`, dates, `fetched_at`, `latest_observed_at`, `points`, `days`, `summary`, `hourly_profile`, `runs`.

Each point contains exact half-open `start`/`end` bounds, `energy_kwh`, `observed_samples`, `expected_samples` and `quality` (`complete`, `partial`, `missing`). A complete measured zero is `0`. Missing energy is `null`. Partial energy is the sum of available valid samples, with coverage flagged; it is never extrapolated to a full bucket. Negative, nonfinite or off-grid source samples are rejected. Rows marked invalid by ingestion are excluded.

`days` carries the same daily coverage and daily energy, plus `productive_hours`. `summary` contains total energy, mean daily energy over days with observations, maximum daily energy, maximum interval energy, productive hours and counts of observed/partial/missing days. These are server statistics; the daily mean does not change bucket aggregation from sum. Partial days contribute their observed energy and are explicitly counted.

`hourly_profile` contains total energy for each Athens clock hour across the requested period and its server-calculated percentage of total energy. `runs` identifies the selected forecast run and generation instant for each available date. `latest_observed_at` is populated only for history; forecasts use `runs[].generated_at` for provenance. `fetched_at` retains the oldest prepared-day fetch instant used in a response, including cache hits.

## Efficiency

One bounded PostgreSQL source scan and `GROUPING SETS` produce bucket, daily and clock-hour energy aggregates. The service combines prepared per-day pieces for bins that cross midnight. It does not send raw five-minute power readings to applications.

A bounded in-process cache keeps up to 360 prepared day/source/interval entries for five minutes. Normalized intervals share keys; overlapping requests reuse cached dates and share in-flight work. A cached or in-flight `1h` request also supplies `day` reads. Historical and forecast entries are separate. This cache contains shared PV installation data, and every HTTP request still passes authentication. HTTP responses use `Cache-Control: private, no-store`.

The data query has a 15-second statement timeout. The latest-forecast lookup is separately bounded at five seconds. No external FusionSolar call, scheduled forecast run, persistent rollup or database schema change occurs during a request.

## Consumers and compatibility

- Web production charts consume hourly `energy_kwh`; totals, daily maxima and the hourly profile come directly from the server. Home forecasts also use the new energy response. Power labels and power integration are removed from these active views. Forecasts remain visually separate and appear only for wholly unmeasured days; they never replace a measured zero or a partially measured day.
- iOS production and Home measured-production consumers use the same history contract. Daily cards use `day`, selected-day charts use `1h`. Highest daily energy replaces peak power; the hourly chart shows kWh. Unit and kind validation reject an incompatible power payload. The existing Athens display/calendar logic is preserved.
- The Render backend needs no code change for these app PV requests, which go directly to the VPS monitoring API. Existing Render/low-level PV and research adapters remain compatible. No unrelated research/export contract is changed.
- `/energy/production/solar/active-power`, `/energy/production/solar/aggregates` and `/energy/production/solar/day-ahead-forecasts` were removed on 2026-09-06 and return 404. They have no redirect or compatibility handler. The separate low-level `/pv/readings`, `/pv/readings/bounds` and `/pv/day-ahead/range` service contracts remain available for existing backend/research consumers.
- The existing Caddy `/energy/*` rule already covers the new paths; no Caddy edit is required for this implementation.

## Validation and release boundary

Real SQL tests use a disposable local PostgreSQL database: integration, energy conservation across intervals, missing/zero/partial observations, 23/25-hour days, forecast run selection, compatibility, authentication, parameter validation and cache reuse. Web tests cover energy contracts and presentation; a local browser preview checks multiple dates, zero/partial/missing days and separate forecast lines. iOS has service/model tests and a Simulator UI test with screenshots showing kWh and the replacement daily maximum.

See `backups/pv-energy-implementation-20260906/REPORT.md` for exact test results, baseline failures and source checksums. The source snapshots preserve the already-dirty checkout as it existed before this task's edits.

The current VPS release is backed up and verified: 50 candidate checks and 43 live checks confirm retirement of the three legacy routes and unchanged supported responses. The web remains live and was rechecked in the authenticated School 10 session after retirement. Render needs no new deployment because these PV requests go directly to the VPS. The isolated iOS 1.0.0 build 4 passed unit and PV Simulator UI tests and signature verification; it has not been installed on a physical iPhone, and the user explicitly accepted the impact on its old PV views. The retirement rollback restores the preceding API image with both new and old routes, so current web and build 4 remain compatible. Rolling the web back to a pre-energy release requires restoring legacy API compatibility first. No database migration or collector restart was needed.
