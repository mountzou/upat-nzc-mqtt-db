# upat-nzc-mqtt-db

Backend and data-services stack for the SchoolHeroZ Digital Twin. It ingests UPAT and Shelly MQTT telemetry, stores measurements and derived data in PostgreSQL, runs scheduled aggregation and forecasting jobs, and exposes the data through a FastAPI service.

## Project structure

This project is organized into service directories, each implementing a core part of the system.

- `/db`: PostgreSQL schema and initialization scripts
- `/api`: FastAPI retrieval service
- `/ttn-ingestor`: MQTT ingestor for UPAT environmental devices
- `/shelly-ingestor`: MQTT ingestor for Shelly energy devices
- `/energy-aggregator`: one-shot Shelly hourly energy aggregation job
- `/simulation-recorder`: one-shot daily simulation recorder
- `/pv-prediction`: one-shot day-ahead PV forecasting job
- `/weather-collector`: one-shot Open-Meteo hourly weather forecast collector
- `/mosquitto`: Mosquitto broker configuration for Shelly message ingestion
- `/caddy`: production HTTPS reverse-proxy configuration

## Project setup

This project follows a container-based architecture, where each core service is built from its own Docker image and runs as an independent container managed through Docker Compose.

Each Python service directory contains its own `Dockerfile`. At the project root, `docker-compose.yml` defines the local development setup, while `docker-compose.prod.yml` defines the production deployment setup.

## Environment variables

Before starting the services, copy `.env.example` to `.env` and fill in the required PostgreSQL, MQTT, TTN, API, forecasting, and collector settings:

```bash
cp .env.example .env
```

These variables are used by `docker-compose.yml` for local development and by `docker-compose.prod.yml` for production deployment.

Do not commit `.env`. Production requires an `OPS_TELEMETRY_TOKEN` of at least 32 characters; keep its value in the deployment environment and out of logs and documentation.

## Getting started

Clone the repository:

```bash
git clone https://github.com/mountzou/upat-nzc-mqtt-db.git
cd upat-nzc-mqtt-db
```

Start the local services with Docker Compose:

```bash
docker compose up -d --build
```

This starts the long-running services and also executes the non-profiled one-shot `energy-aggregator` and `simulation-recorder` containers once. The `pv-prediction` and `weather-collector` jobs are enabled only through the `jobs` profile.

Check that the containers are running:

```bash
docker compose ps
```

Inspect the service logs:

```bash
docker compose logs --tail=50 postgres
docker compose logs --tail=50 ttn-ingestor
docker compose logs --tail=50 shelly-ingestor
docker compose logs --tail=50 api
```

## Energy aggregator

The `energy-aggregator` service is a one-shot job. It processes the most recently completed UTC hour and upserts per-device energy into `shelly_plug_hourly_energy` and `shelly_pro3em_hourly_energy`. Working-day and working-hour flags are evaluated in the `Europe/Athens` timezone.

Run it manually:

```bash
docker compose run --rm energy-aggregator
```

## Simulation recorder

The `simulation-recorder` service is a one-shot container intended to be run by VPS cron. It signs in to the simulation backend with the dedicated `SIMULATION_API_USERNAME` and `SIMULATION_API_PASSWORD` service credentials, uses the temporary bearer token for the run, and calls the backend with an explicit `Europe/Athens` D+1 `target_date`. It records the raw execution response in PostgreSQL and stores one extracted room recording per returned item. The raw response preserves the quality-gated 24-hour `hourly_load` profile for the optimization adapter. Missing or rejected credentials stop the recorder before it opens a database connection or creates a run record; credential and token values are never logged.

The simulation endpoint, recording timezone, and supported school allowlist are defined in `simulation-recorder/main.py`. The schools processed by a run can be narrowed with the comma-separated `SIMULATION_SCHOOL_IDS` environment variable; every configured ID must belong to the supported allowlist.

```text
base URL: https://upat-nzc-energyplus-backend.onrender.com
path: /simulate/day-ahead
supported schools: school_3, school_7, school_10, school_13, school_22, school_23
recording timezone: Europe/Athens
```

Run it manually:

```bash
docker compose run --rm simulation-recorder
```

For an existing PostgreSQL volume, apply the idempotent day-ahead result migration before the first run:

```bash
docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/migrations/002_simulation_day_ahead_results.sql'
```

## PV prediction

The `pv-prediction` service is a one-shot D+1 forecasting job. It fetches hourly Open-Meteo inputs, builds the model features, loads the tracked model artifacts, and optionally stores the 24 hourly predictions in PostgreSQL when `PV_SAVE_TO_DB=true`.

The tracked operational model is `rf_operational_20260806`, a `RandomForestRegressor` trained on a short summer reference dataset (39 dates and 903 rows). Its feature contract and provenance are recorded in `pv-prediction/pv_model_manifest_rf_operational_20260806.json`; the associated model and feature artifacts are versioned in the same directory. Treat this model as an operational evaluation baseline rather than a fully validated year-round model.

The current Random Forest feature contract excludes `lag_1h`. `PV_LATEST_ACTIVE_POWER_KW`, `PV_LAG_1H_KW`, and the corresponding nullable database fields are retained only for schema and CLI compatibility; changing them does not affect current RF predictions. When neither legacy value is explicitly supplied, new RF rows store `NULL` rather than manufacturing a measured-power value.

Open-Meteo collection retries transient network and HTTP failures up to four attempts with 10, 20, and 40 second backoff delays. Retries happen before inference and database persistence, so a failed weather request cannot create duplicate forecast rows.

Preview a forecast without writing to PostgreSQL:

```bash
docker compose --profile jobs run --rm \
  -e PV_SAVE_TO_DB=false \
  pv-prediction --no-save-to-db
```

Run the normal persisted job:

```bash
docker compose --profile jobs run --rm pv-prediction
```

The normal command writes to PostgreSQL when `PV_SAVE_TO_DB=true`, which is the default in the Compose service. Use the preview command for manual validation.

For an existing PostgreSQL volume, apply the idempotent PV forecast migration before saving the first run:

```bash
docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/migrations/003_pv_day_ahead_forecasts.sql'
```

## Weather collector

The `weather-collector` service is a one-shot container intended to be run by VPS cron. It fetches an 8-day hourly forecast from Open-Meteo for the configured latitude/longitude and permanently upserts the rows into `weather_hourly_forecasts`. The extra day keeps the API's current 7-day simulation window complete after midnight and before the next nightly collector run.

Default configuration:

```text
latitude: 37.068
longitude: 22.026
timezone: Europe/Athens
wind speed unit: ms
forecast window: 8 local dates, from today through today + 7 days
```

Run it manually:

```bash
docker compose --profile jobs run --rm weather-collector
```

For an existing PostgreSQL volume, apply the idempotent weather forecast migration before the first run:

```bash
docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/migrations/004_weather_hourly_forecasts.sql'
```

## Persistence-free PV ingestion preview

The `pv-ingestor` service is available in both Compose files only through the
explicit `pv-ingestor-preview` profile. It performs sequential FusionSolar read
calls, validates and post-processes the returned device data, and stops before
persistence. It has no PostgreSQL or Firestore configuration, dependency,
volume, or exposed port.

On the production host, build and execute one completed Athens-local day
manually with:

```bash
docker compose -f docker-compose.prod.yml --profile pv-ingestor-preview build pv-ingestor
docker compose -f docker-compose.prod.yml --profile pv-ingestor-preview run --rm --no-deps pv-ingestor --live --lookback-days 1
```

This preview remains intentionally absent from the production cron entries.
The separate persistent production path uses the reviewed
`upat-pv-ingestor.service` and `upat-pv-ingestor.timer` units under
`ops/systemd/`; see `ops/README.md`. Do not enable that timer while any legacy
FusionSolar scheduler is active.

## Production day-ahead schedule

A recommended production order for the day-ahead jobs is below, using `Europe/Athens` wall-clock time throughout the year:

1. `22:50` — refresh the Open-Meteo weather forecast.
2. `23:00` — generate and persist the D+1 PV forecast.
3. `23:10` — run and persist the D+1 EnergyPlus demand simulation for `school_10`.

The cron daemon invokes each entry every minute, while an explicit `TZ=Europe/Athens` time guard selects the intended local time across daylight-saving changes. `flock` prevents overlapping runs. These are the repository-recommended entries; verify them against the live VPS crontab before applying changes:

```cron
* * * * * /usr/bin/env TZ=Europe/Athens /bin/sh -c '[ "$(/bin/date +\%H:\%M)" = "22:50" ] || exit 0; cd /opt/upat-nzc-mqtt-db && /usr/bin/flock -n /var/lock/weather-collector.lock /usr/bin/docker compose -f docker-compose.prod.yml --profile jobs run --rm --no-deps weather-collector' >> /var/log/weather-collector.log 2>&1
* * * * * /usr/bin/env TZ=Europe/Athens /bin/sh -c '[ "$(/bin/date +\%H:\%M)" = "23:00" ] || exit 0; cd /opt/upat-nzc-mqtt-db && /usr/bin/flock -n /var/lock/pv-prediction.lock /usr/bin/docker compose -f docker-compose.prod.yml --profile jobs run --rm --no-deps pv-prediction' >> /var/log/pv_prediction.log 2>&1
* * * * * /usr/bin/env TZ=Europe/Athens /bin/sh -c '[ "$(/bin/date +\%H:\%M)" = "23:10" ] || exit 0; cd /opt/upat-nzc-mqtt-db && /usr/bin/flock -n /var/lock/simulation-recorder.lock /usr/bin/docker compose -f docker-compose.prod.yml run --rm --no-deps -e SIMULATION_SCHOOL_IDS=school_10 simulation-recorder' >> /var/log/simulation_recorder.log 2>&1
```

The recommended PV entry intentionally omits the legacy lag variables, so new RF rows store `NULL` in those fields unless a real value is supplied explicitly.

## API service

The proposed database-backed application-user boundary is documented in
[`api/AUTH_SERVICE.md`](api/AUTH_SERVICE.md). It is fail-closed and remains
inactive until its migration, service secret, and Render adapter are rolled out
in separately approved batches.

Base URL in local development:

```text
http://localhost:8000
```

### `GET /health`

Checks that the API is running and can connect to Postgres.

Example:

```bash
curl -s http://localhost:8000/health
```

Example response:

```json
{
  "status": "ok",
  "database": "connected"
}
```

### `GET /ops/telemetry`

Returns sanitized fleet, runtime, and PostgreSQL operational telemetry. The endpoint requires a bearer token matching `OPS_TELEMETRY_TOKEN` and does not return raw measurement payloads or credential values.

Example:

```bash
curl -s \
  -H "Authorization: Bearer $OPS_TELEMETRY_TOKEN" \
  http://localhost:8000/ops/telemetry
```

The configured token must be at least 32 characters. An unconfigured or too-short server token returns `503`; a missing or incorrect bearer token returns `401`.

### `GET /upat/devices`

Returns all known environmental devices from the `upat_devices` table.

Example:

```bash
curl -s http://localhost:8000/upat/devices
```

Example response:

```json
[
  {
    "id": 1,
    "source": "ttn",
    "device_id": "portable-112",
    "dev_eui": "ABC123...",
    "name": "portable-112",
    "created_at": "2026-03-14T13:00:00"
  }
]
```

### `GET /shelly/devices`

Returns all known Shelly devices from the `shelly_devices` table.

Example:

```bash
curl -s http://localhost:8000/shelly/devices
```

### `GET /simulations/recordings/latest`

Returns the latest successful stored room configuration recording for a school. This endpoint is retained for compatibility; use `/simulations/day-ahead/latest` for actual simulation result metrics.

Query parameters:

- `school_id`
  Required. School identifier, for example `school_10`.

Example:

```bash
curl -s "http://localhost:8000/simulations/recordings/latest?school_id=school_10"
```

Example response:

```json
{
  "school_id": "school_10",
  "recording_date": "2026-04-27",
  "count": 8,
  "items": [
    {
      "room_id": "classroom",
      "label": "Classroom",
      "physical_instance_count": 8,
      "zone_name": "Classroom",
      "thermostat_type": "single_heating"
    }
  ]
}
```

### `GET /simulations/day-ahead/latest`

Returns the latest stored day-ahead EnergyPlus simulation results for a school.

Query parameters:

- `school_id`
  Required. School identifier, for example `school_10`.

Example:

```bash
curl -s "http://localhost:8000/simulations/day-ahead/latest?school_id=school_10"
```

Example response:

```json
{
  "status": "success",
  "simulation_engine": "energyplus",
  "run_id": "20260425_162646_c373b453",
  "school_id": "school_10",
  "summary": {
    "requested_rooms": 8,
    "successful_rooms": 8,
    "failed_rooms": 0
  },
  "day_ahead_date": "2026-04-25",
  "school_totals": {
    "facility_kwh": 83.7,
    "equipment_kwh": 20.7,
    "lighting_kwh": 40.96,
    "heating_liters": 0,
    "cooling_kwh": 19.87,
    "fans_hvac_kwh": 2.16
  },
  "room_results": [
    {
      "room_id": "classroom",
      "room_label": "Classroom × 8",
      "status": "success",
      "metrics": {
        "average_air_temperature_c": 24.8,
        "thermal_discomfort_hours": 3.67,
        "facility_kwh": 22.5,
        "equipment_kwh": 0,
        "lighting_kwh": 22.5,
        "heating_liters": 0,
        "cooling_kwh": 0,
        "fans_hvac_kwh": 0
      }
    }
  ]
}
```

### `GET /pv/day-ahead/latest`

Returns the latest successful stored D+1 PV forecast and its native hourly predictions. This is a latest-only endpoint; it does not expose historical forecast runs.

Example:

```bash
curl -s http://localhost:8000/pv/day-ahead/latest
```

The response includes `run_id`, `forecast_date`, `daily_energy_kwh`, model and feature artifact names, 24 ordered hourly `items`, and recording timestamps. Each hourly item contains `timestamp`, `hour`, and `predicted_power_kw`.

### `GET /weather/hourly/forecast`

Returns stored Open-Meteo hourly forecasts. If `start` and `end` are omitted, the endpoint returns the default 7-day local forecast window from today through today + 6 days.

Query parameters:

- `start`
  Optional. `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM`.
- `end`
  Optional. `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM`.

Example:

```bash
curl -s "http://localhost:8000/weather/hourly/forecast?start=2026-05-25&end=2026-05-31"
```

### `GET /weather/hourly/latest`

Returns the nearest stored current or future hourly weather forecast row.

Example:

```bash
curl -s http://localhost:8000/weather/hourly/latest
```

### `GET /upat/device/{device_id}/latest`

Returns the latest 30 one-minute aggregated measurement snapshots for the selected device by default.

Query parameters:

- `metric`
  Optional. Repeat the parameter to request multiple metrics, for example `?metric=temperature&metric=relative_humidity`.
- `limit`
  Optional. Number of grouped items to return. Default: `30`. Maximum: `1000`.

Example:

```bash
curl -s "http://localhost:8000/upat/device/portable-112/latest"
```

Filter specific metrics:

```bash
curl -s "http://localhost:8000/upat/device/portable-112/latest?metric=temperature&metric=relative_humidity&limit=10"
```

Example response:

```json
{
  "device_id": "portable-112",
  "count": 2,
  "items": [
    {
      "device_id": "portable-112",
      "event_time": "2026-03-14T14:02:00",
      "measurements": {
        "relative_humidity": {
          "value": 60.4,
          "unit": "%"
        },
        "temperature": {
          "value": 16.0,
          "unit": "C"
        }
      }
    }
  ]
}
```

### `GET /upat/device/{device_id}/history`

Returns historical aggregated environmental measurements for a single device from `upat_measurements`.

Query parameters:

- `metric`
  Optional. Repeat the parameter to request multiple metrics.
- `limit`
  Optional. Number of grouped items to return. Default: `100`. Maximum: `1000`.
- `start`
  Optional. Start bound.
  Accepted formats:
  - `YYYY-MM-DD`
  - `YYYY-MM-DDTHH:MM`
- `end`
  Optional. End bound.
  Accepted formats:
  - `YYYY-MM-DD`
  - `YYYY-MM-DDTHH:MM`
- `aggregate`
  Optional. Currently supports only `avg`.
- `bucket_unit`
  Optional. Supported values:
  - `minute`
  - `hour`
  - `day`
- `bucket_size`
  Optional. Bucket size. Examples: `1`, `2`, `15`.

Notes:

- If `start` or `end` is provided as `YYYY-MM-DD`, the API expands it to the full day.
- Explicit timestamp ranges use a half-open interval: `start` is included and
  `end` is excluded. This prevents the first bucket of the following period
  from being returned as a partial bucket.
- If aggregation parameters are used, `aggregate=avg` must also be provided.
- `limit` applies only when no explicit `start` and `end` range is provided.
- If no time range is provided, the default history view is the last 1 day aggregated at 1-minute resolution.
- UPAT minute buckets that are exact multiples of five use persisted five-minute
  rollups. Minute buckets that are exact multiples of 60, plus hourly and daily
  buckets, use persisted hourly rollups.
- Rollup-backed UPAT queries add only measurements newer than the atomic rollup
  watermark, so results remain current between rollup jobs without
  double-counting processed measurements.
- Other minute bucket sizes use retained raw measurements and are therefore
  limited to the high-resolution retention window.

#### Aggregated history examples

Latest twelve five-minute averages:

```bash
curl -s "http://localhost:8000/upat/device/portable-112/history?aggregate=avg&bucket_unit=minute&bucket_size=5&limit=12"
```

Latest four fifteen-minute averages:

```bash
curl -s "http://localhost:8000/upat/device/portable-112/history?aggregate=avg&bucket_unit=minute&bucket_size=15&limit=4"
```

Hourly averages:

```bash
curl -s "http://localhost:8000/upat/device/portable-112/history?aggregate=avg&bucket_unit=hour&bucket_size=1&limit=24"
```

Two-hour averages:

```bash
curl -s "http://localhost:8000/upat/device/portable-112/history?aggregate=avg&bucket_unit=hour&bucket_size=2&limit=24"
```

Daily averages:

```bash
curl -s "http://localhost:8000/upat/device/portable-112/history?aggregate=avg&bucket_unit=day&bucket_size=1&limit=7"
```

Filtered aggregated history:

```bash
curl -s "http://localhost:8000/upat/device/portable-112/history?metric=temperature&metric=relative_humidity&aggregate=avg&bucket_unit=hour&bucket_size=2&limit=12"
```

Aggregated history in a time range:

```bash
curl -s "http://localhost:8000/upat/device/portable-112/history?aggregate=avg&bucket_unit=hour&bucket_size=2&start=2026-03-14T00:00&end=2026-03-14T12:00"
```

Example response:

```json
{
  "device_id": "portable-112",
  "count": 2,
  "items": [
    {
      "device_id": "portable-112",
      "event_time": "2026-03-14T12:00:00",
      "measurements": {
        "relative_humidity": {
          "value": 59.6,
          "unit": "%"
        },
        "temperature": {
          "value": 16.0,
          "unit": "C"
        }
      }
    },
    {
      "device_id": "portable-112",
      "event_time": "2026-03-14T10:00:00",
      "measurements": {
        "relative_humidity": {
          "value": 58.1,
          "unit": "%"
        },
        "temperature": {
          "value": 16.0,
          "unit": "C"
        }
      }
    }
  ]
}
```

## Shelly telemetry and energy API

### `GET /shelly/device/{device_id}/latest`

Returns the latest one-minute aggregated Shelly measurement snapshots. It accepts the same repeatable `metric` filter and `limit` parameter as the UPAT latest endpoint; `limit` defaults to `30` and is capped at `1000`.

Example:

```bash
curl -s "http://localhost:8000/shelly/device/shellypro3em-example/latest?metric=active_power&limit=30"
```

### `GET /shelly/device/{device_id}/history`

Returns raw historical Shelly telemetry from `shelly_measurements`. It uses the same `metric`, `start`, `end`, `aggregate`, `bucket_unit`, `bucket_size`, and `limit` query contract described for `/upat/device/{device_id}/history`.

Example:

```bash
curl -s "http://localhost:8000/shelly/device/shellypro3em-example/history?start=2026-08-01&end=2026-08-02&metric=active_power"
```

### Shelly hourly-energy endpoints

The following endpoints read the hourly rows produced by `energy-aggregator`:

- `GET /shelly/hourly-energy` returns hourly rows for one or more devices. Repeat the required `device_id` parameter to select multiple devices.
- `GET /shelly/device/{device_id}/hourly-energy` returns hourly rows for one device.

Both endpoints accept optional `start`, `end`, and `working_only` parameters. Without explicit bounds, they return the most recently completed 24-hour UTC interval. `working_only=true` keeps only rows marked as both a working day and a working hour in `Europe/Athens`.

Example:

```bash
curl -s "http://localhost:8000/shelly/hourly-energy?device_id=shellyplug-example&device_id=shellypro3em-example&start=2026-08-01&end=2026-08-02"
```

### Shelly aggregate-energy endpoints

The following endpoints sum the stored hourly energy rows over the selected interval:

- `GET /shelly/energy` returns totals for one or more devices and requires at least one repeatable `device_id` parameter.
- `GET /shelly/device/{device_id}/energy` returns the total for one device.

They accept the same optional `start`, `end`, and `working_only` parameters and the same default 24-hour UTC interval as the hourly-energy endpoints. Device IDs must start with `shellyplug` or `shellypro3em`. Plug responses expose `energy_wh.total`; Pro 3EM responses also expose the `a`, `b`, and `c` phase totals.

Example:

```bash
curl -s "http://localhost:8000/shelly/device/shellypro3em-example/energy?start=2026-08-01&end=2026-08-02&working_only=true"
```
