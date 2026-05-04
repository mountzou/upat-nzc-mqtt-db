# upat-nzc-mqtt-db

Backend service for the SchoolHeroZ Digital Twin that ingests data from UPAT and Shelly MQTT devices, stores it in PostgreSQL, and exposes retrieval APIs for device measurements.

## Project structure

This project is organized into six directories, each implementing a core service of the system.

- `/db`: PostgreSQL schema and initialization scripts
- `/api`: FastAPI retrieval service
- `/ttn-ingestor`: MQTT ingestor for UPAT environmental devices
- `/shelly-ingestor`: MQTT ingestor for Shelly energy devices
- `/simulation-recorder`: one-shot daily simulation recorder
- `/mosquitto`: Mosquitto broker configuration for Shelly message ingestion

## Project setup

This project follows a container-based architecture, where each core service is built from its own Docker image and runs as an independent container managed through Docker Compose.

Each core service directory contains its own `Dockerfile`. At the project root, `docker-compose.yml` defines the local development setup, while `docker-compose.prod.yml` defines the production deployment setup.

## Environment variables

Before starting the services, create a `.env` file in the project root and define the required environment variables for PostgreSQL, MQTT, TTN, API configuration, and Shelly ingestion.

These variables are used by `docker-compose.yml` for local development and by `docker-compose.prod.yml` for production deployment.

A sample `env.local` file is included in the project root to help users populate the required environment variables during setup.

## Getting started

Clone the repository:

```bash
git clone https://github.com/mountzou/upat-nzc-mqtt-db.git
cd upat-nzc-mqtt-db
```

Start the local services with Docker Compose:

```
docker compose up -d --build
```

Check that the containers are running:

```
docker compose ps
```

Inspect the service logs:

```
docker compose logs --tail=50 postgres
docker compose logs --tail=50 ttn-ingestor
docker compose logs --tail=50 shelly-ingestor
docker compose logs --tail=50 api
```

## Simulation recorder

The `simulation-recorder` service is a one-shot container intended to be run by VPS cron. It calls the simulation backend, records the raw execution response in PostgreSQL, and stores one extracted room recording per returned item.

The first version uses constants in `simulation-recorder/main.py`:

```text
base URL: https://upat-nzc-energyplus-backend.onrender.com
path: /simulate/day-ahead
schools: school_3, school_7, school_10, school_13, school_22, school_23
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

## API service

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
- If aggregation parameters are used, `aggregate=avg` must also be provided.
- `limit` applies only when no explicit `start` and `end` range is provided.
- If no time range is provided, the default history view is the last 1 day aggregated at 1-minute resolution.

#### Aggregated history examples

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
