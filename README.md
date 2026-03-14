# upat-nzc-mqtt-db

Small IoT backend for ingesting MQTT sensor data into Postgres and exposing it through a FastAPI service.

## API

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

### `GET /devices`

Returns all known devices from the `devices` table.

Example:

```bash
curl -s http://localhost:8000/devices
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

### `GET /devices/{device_id}/latest`

Returns the latest `30` one-minute aggregated measurement groups for the selected device by default.

Query parameters:

- `metric`
  Optional. One metric or a comma-separated list, for example `temperature` or `temperature,relative_humidity`.
- `limit`
  Optional. Number of grouped items to return. Default: `30`. Maximum: `1000`.

Example:

```bash
curl -s "http://localhost:8000/devices/portable-112/latest"
```

Filter specific metrics:

```bash
curl -s "http://localhost:8000/devices/portable-112/latest?metric=temperature,relative_humidity&limit=10"
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

### `GET /devices/{device_id}/history`

Returns grouped measurements for one device. It can return:

- raw grouped measurements by original `event_time`
- aggregated grouped measurements using time buckets

### `GET /device/{device_id}/history`

Alias of the same history endpoint above.

Query parameters:

- `metric`
  Optional. One metric or a comma-separated list.
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
- `bucket`
  Optional. Legacy bucket unit shortcut.
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
- `limit` applies to grouped timestamps or grouped time buckets, not individual measurement rows.

#### Raw history examples

Latest raw grouped measurements:

```bash
curl -s "http://localhost:8000/devices/portable-112/history?limit=10"
```

Raw history for selected metrics:

```bash
curl -s "http://localhost:8000/devices/portable-112/history?metric=temperature,relative_humidity&limit=10"
```

Raw history for a full day:

```bash
curl -s "http://localhost:8000/devices/portable-112/history?start=2026-03-14&end=2026-03-14"
```

Raw history for a specific time window:

```bash
curl -s "http://localhost:8000/devices/portable-112/history?start=2026-03-14T08:00&end=2026-03-14T12:00"
```

#### Aggregated history examples

Hourly averages:

```bash
curl -s "http://localhost:8000/devices/portable-112/history?aggregate=avg&bucket_unit=hour&bucket_size=1&limit=24"
```

Two-hour averages:

```bash
curl -s "http://localhost:8000/devices/portable-112/history?aggregate=avg&bucket_unit=hour&bucket_size=2&limit=24"
```

Daily averages:

```bash
curl -s "http://localhost:8000/devices/portable-112/history?aggregate=avg&bucket_unit=day&bucket_size=1&limit=7"
```

Filtered aggregated history:

```bash
curl -s "http://localhost:8000/devices/portable-112/history?metric=temperature,relative_humidity&aggregate=avg&bucket_unit=hour&bucket_size=2&limit=12"
```

Aggregated history in a time range:

```bash
curl -s "http://localhost:8000/devices/portable-112/history?aggregate=avg&bucket_unit=hour&bucket_size=2&start=2026-03-14T00:00&end=2026-03-14T12:00"
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

## Development

Start the stack locally:

```bash
docker compose up --build
```

Rebuild just the API service:

```bash
docker compose up -d --build api
```
