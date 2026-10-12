# PV production energy contract

Authenticated reads use the monitoring user JWT and shared PV installation
access policy. They read stored PostgreSQL data; FusionSolar collection is
documented in [the collector guide](../jobs/collect-pv/README.md).

## Requests

```http
GET /energy/production/history?start_date=2026-09-01&end_date=2026-09-05&interval=1h
GET /energy/production/forecasts?start_date=2026-09-07&end_date=2026-09-07&interval=1h
GET /energy/production/forecasts?latest=true&interval=1h
```

| Parameter | Contract |
| --- | --- |
| `start_date`, `end_date` | Inclusive Athens calendar dates; supplied together, maximum 90 days. |
| `interval` | Default `1h`. History accepts multiples of `5m`; forecasts accept multiples of `1h`; maximum fixed duration `168h`. Both accept `day`. `60m` normalizes to `1h`. |
| `latest=true` | Forecasts only; selects the target date of the latest successful stored run. Cannot accompany dates. |

Unknown/duplicate parameters and unsupported intervals return 422. There are no
metric, aggregate, resolution, timezone or legacy bucket selectors. A missing
latest forecast returns 404; dated ranges without data return missing series.
Database failures return 503; invalid or duplicate source intervals return 502.

## Energy and time

History converts each valid `pv_plant_readings_5m` observation to
`active_power_kw × (5 / 60)` kWh. Forecasts use the latest successful stored run
per target date and `predicted_power_kw × 1` kWh per hour. Buckets sum energy;
coarser intervals preserve the total for the same requested dates. No power
fields are returned.

Fixed intervals use UTC origin `2001-01-01T00:00:00Z`. Edge bins are clipped to
the requested date range; their half-open start/end bounds are authoritative.
`day` follows Athens midnight and lasts 23/24/25 hours across DST. Fixed `24h`
means elapsed time. Response instants use UTC `Z`; `Europe/Athens` is fixed
calendar/display metadata.

Measured timestamps use TIMESTAMPTZ. Forecast timestamps have a separate Athens
local-civil TIMESTAMP contract, interpreted once when reading. Nonexistent
spring hours are excluded. A 24-row autumn forecast cannot represent both
repeated hours: the absent hour remains missing and the day is partial.

## Response and quality

Common fields: `source=postgres`, `kind=history|forecast`, `unit=kWh`,
`timezone`, `interval`, `start_date`, `end_date`, `fetched_at`,
`latest_observed_at`, `points`, `days`, `summary`, `hourly_profile`, `runs`.

- Points contain start/end, `energy_kwh`, observed/expected sample counts and
  `quality=complete|partial|missing`.
- Measured zero stays 0; missing energy is null. Partial energy is the observed
  subtotal, never extrapolated. Negative, nonfinite and off-grid samples fail
  validation; ingestion rows marked invalid are excluded.
- Days include energy, coverage and productive hours. Summary includes total,
  mean/max daily energy, maximum interval energy, productive hours and
  observed/partial/missing day counts. Partial days contribute their observed
  energy; the daily mean uses days with observations.
- Hourly profile contains energy by Athens clock hour and its percentage of
  total energy. Forecast runs identify the selected run and generation instant.
  `latest_observed_at` applies only to history; `fetched_at` is the oldest
  prepared-day fetch instant, including cache hits.

Clients use server totals/profiles and plot `energy_kwh`. Forecasts stay
separate and do not replace measured zero or partially measured days.

## Query limits and compatibility

A bounded SQL scan with GROUPING SETS computes bucket, daily and clock-hour
statistics. The cache holds at most 360 day/source/interval entries for five
minutes, coalesces overlapping work and lets hourly entries serve daily reads.
History and forecasts have separate keys; every request still authenticates.
HTTP responses use `Cache-Control: private, no-store`.

Data queries have a 15-second statement timeout; latest-forecast lookup has five
seconds. HTTP reads trigger no provider calls, forecast job, rollup or migration.

The former active-power, aggregates and day-ahead-forecasts routes under
`/energy/production/solar/` are retired (404), without redirects. Protected
low-level `/pv/*` readers remain separate contracts. Recovery uses a verified
image/configuration pair compatible with current consumers and the
[current storage/time requirements](shelly-compact/RECOVERY.md).

Contract tests live in [test_pv_energy.py](../api/tests/test_pv_energy.py).
Historical rollout notes remain in Git history.
