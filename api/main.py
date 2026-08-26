import hmac
import os
import shutil
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Annotated
from zoneinfo import ZoneInfo

import psycopg2
from auth_service import AuthVerifyRateLimiter, build_auth_router
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from psycopg2.extras import RealDictCursor
from schemas import HistoryQueryParams, normalize_metrics, parse_datetime_bound

app = FastAPI()


@app.exception_handler(RequestValidationError)
async def redact_auth_request_validation_error(
    request: Request,
    exc: RequestValidationError,
):
    if request.url.path.startswith("/internal/auth/"):
        return JSONResponse(
            status_code=422,
            content={"detail": "Invalid authentication request."},
            headers={"Cache-Control": "no-store"},
        )

    return await request_validation_exception_handler(request, exc)


DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_INTERNAL_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
OPS_TELEMETRY_TOKEN = os.getenv("OPS_TELEMETRY_TOKEN", "").strip()
AUTH_SERVICE_TOKEN = os.getenv("AUTH_SERVICE_TOKEN", "").strip()
AUTH_VERIFY_RATE_LIMITER = AuthVerifyRateLimiter()
WEATHER_TIMEZONE = os.getenv("OPEN_METEO_TIMEZONE", "Europe/Athens")
WEATHER_LOCAL_TZ = ZoneInfo(WEATHER_TIMEZONE)

WEATHER_FIELDS = [
    "temperature_2m_c",
    "dew_point_2m_c",
    "relative_humidity_2m_percent",
    "surface_pressure_hpa",
    "shortwave_radiation_w_m2",
    "direct_normal_irradiance_w_m2",
    "diffuse_radiation_w_m2",
    "wind_direction_10m_degrees",
    "wind_speed_10m_ms",
    "weather_code",
    "snow_depth_m",
    "precipitation_mm",
    "cloud_cover_percent",
]

# Keep the public weather API contract unit-explicit while the database uses
# the exact Open-Meteo variable names. The SQL projections below own this
# compatibility boundary.

# Create and return a new PostgreSQL connection.
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        cursor_factory=RealDictCursor,
    )


app.include_router(
    build_auth_router(
        connection_factory=lambda: get_connection(),
        service_token_getter=lambda: AUTH_SERVICE_TOKEN,
        verify_rate_limiter=AUTH_VERIFY_RATE_LIMITER,
    )
)


def round_numeric(value):
    if isinstance(value, (int, float)) and value is not None:
        return round(value, 1)
    return value


def normalize_device_ids(device_ids: list[str] | None):
    if not device_ids:
        return None
    return sorted({d.strip() for d in device_ids if d and d.strip()}) or None


def normalize_required_text(value: str, field_name: str):
    normalized = value.strip() if value else ""

    if not normalized:
        raise HTTPException(
            status_code=400,
            detail=f"{field_name} must be provided",
        )

    return normalized


def numeric_or_none(value):
    if value is None:
        return None

    if isinstance(value, Decimal):
        return float(value)

    return value


def format_weather_forecast_hour(row):
    return {
        "timestamp": row["forecast_timestamp"],
        "date": row["forecast_date"],
        "hour": row["forecast_hour"],
        "values": {
            field: numeric_or_none(row[field])
            for field in WEATHER_FIELDS
        },
        "source": row["source"],
        "latitude": numeric_or_none(row["latitude"]),
        "longitude": numeric_or_none(row["longitude"]),
        "timezone": row["timezone"],
        "fetched_at": row["fetched_at"],
    }


def resolve_weather_time_bounds(start: str | None, end: str | None):
    if (start is None) != (end is None):
        raise HTTPException(
            status_code=400,
            detail="start and end must either both be provided or both be omitted",
        )

    if start is not None and end is not None:
        start_time = parse_datetime_bound(start, "start")
        end_time = parse_datetime_bound(end, "end")
    else:
        today = datetime.now(WEATHER_LOCAL_TZ).date()
        start_time = datetime.combine(today, time.min)
        end_time = datetime.combine(today + timedelta(days=6), time.max)

    if start_time > end_time:
        raise HTTPException(
            status_code=400,
            detail="start must be earlier than or equal to end",
        )

    return start_time, end_time


def resolve_energy_time_bounds(start: str | None, end: str | None):
    now = datetime.utcnow()
    default_end = now.replace(minute=0, second=0, microsecond=0)
    default_start = default_end - timedelta(hours=24)

    start_time = parse_datetime_bound(start, "start") if start is not None else default_start
    end_time = parse_datetime_bound(end, "end") if end is not None else default_end

    if start_time > end_time:
        raise HTTPException(
            status_code=400,
            detail="start must be earlier than or equal to end",
        )

    return start_time, end_time


def get_shelly_device_db_table(device_id: str):
    if device_id.startswith("shellyplug"):
        return {
            "device_type": "plug",
            "table_name": "shelly_plug_hourly_energy",
        }

    if device_id.startswith("shellypro3em"):
        return {
            "device_type": "pro3em",
            "table_name": "shelly_pro3em_hourly_energy",
        }

    raise HTTPException(
        status_code=400,
        detail=f"Unknown Shelly device type for device_id={device_id}",
    )


def split_shelly_device_ids(device_ids: list[str]):
    plug_ids    = [d for d in device_ids if d.startswith("shellyplug")]
    pro3em_ids  = [d for d in device_ids if d.startswith("shellypro3em")]
    unknown_ids = [d for d in device_ids if d not in plug_ids and d not in pro3em_ids]

    if unknown_ids:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown Shelly device type for device_ids={unknown_ids}",
        )

    return plug_ids, pro3em_ids


def format_response_object(device_id, rows, metrics=None):
    snapshots = []
    snapshots_by_time = {}

    for row in rows:
        event_time = row["event_time"]
        event_time_key = event_time.isoformat() if event_time else "null"

        if metrics and row["metric"] not in metrics:
            continue

        if event_time_key not in snapshots_by_time:
            snapshot = {
                "device_id": row["device_id"],
                "event_time": event_time,
                "measurements": {},
            }
            snapshots_by_time[event_time_key] = snapshot
            snapshots.append(snapshot)

        snapshots_by_time[event_time_key]["measurements"][row["metric"]] = {
            "value": round_numeric(row["value"]),
            "unit": row["unit"],
        }

    return {
        "device_id": device_id,
        "count": len(snapshots),
        "items": snapshots,
    }


def format_simulation_recording(row):
    return {
        "id": row["id"],
        "run_id": row["run_id"],
        "school_id": row["school_id"],
        "recording_date": row["recording_date"],
        "room_id": row["room_id"],
        "label": row["label"],
        "physical_instance_count": row["physical_instance_count"],
        "idf_file": row["idf_file"],
        "zone_name": row["zone_name"],
        "thermostat_type": row["thermostat_type"],
        "supports": {
            "cooling_setpoint": row["supports_cooling_setpoint"],
        },
        "defaults": {
            "occupancy": row["default_occupancy"],
            "heating_setpoint": numeric_or_none(row["default_heating_setpoint"]),
            "cooling_setpoint": numeric_or_none(row["default_cooling_setpoint"]),
            "lighting_w_per_m2": numeric_or_none(row["default_lighting_w_per_m2"]),
            "infiltration_ach": numeric_or_none(row["default_infiltration_ach"]),
        },
        "raw_item": row["raw_item"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def format_day_ahead_room_result(row):
    result = {
        "room_id": row["room_id"],
        "room_label": row["room_label"],
        "status": row["status"],
    }

    if row["status"] == "success":
        result["metrics"] = {
            "average_air_temperature_c": numeric_or_none(row["average_air_temperature_c"]),
            "thermal_discomfort_hours": numeric_or_none(row["thermal_discomfort_hours"]),
            "facility_kwh": numeric_or_none(row["facility_kwh"]),
            "equipment_kwh": numeric_or_none(row["equipment_kwh"]),
            "lighting_kwh": numeric_or_none(row["lighting_kwh"]),
            "heating_liters": numeric_or_none(row["heating_liters"]),
            "cooling_kwh": numeric_or_none(row["cooling_kwh"]),
            "fans_hvac_kwh": numeric_or_none(row["fans_hvac_kwh"]),
        }

    if row["error_text"] is not None:
        result["error"] = row["error_text"]

    return result


def format_pv_forecast_hour(row):
    return {
        "timestamp": row["forecast_timestamp"],
        "hour": row["forecast_hour"],
        "predicted_power_kw": numeric_or_none(row["predicted_power_kw"]),
    }


def fetch_device_latest(table_name, device_id, metrics, limit):
    normalized_metrics = normalize_metrics(metrics)

    query_params = []
    query_parts = [f"""
        WITH aggregated AS (
            SELECT
                device_id,
                metric,
                AVG(value) AS value,
                unit,
                date_bin(
                    INTERVAL '1 minute',
                    event_time,
                    TIMESTAMP '2001-01-01 00:00:00'
                ) AS bucket_time
            FROM {table_name}
            WHERE device_id = %s
    """]
    query_params.append(device_id)

    if normalized_metrics:
        query_parts.append(" AND metric = ANY(%s)")
        query_params.append(normalized_metrics)

    query_parts.append("""
            GROUP BY device_id, metric, unit, bucket_time
        ),
        selected_times AS (
            SELECT DISTINCT bucket_time
            FROM aggregated
            ORDER BY bucket_time DESC
            LIMIT %s
        )
        SELECT
            device_id,
            metric,
            value,
            unit,
            bucket_time AS event_time
        FROM aggregated
        WHERE bucket_time IN (SELECT bucket_time FROM selected_times)
        ORDER BY bucket_time DESC, metric ASC;
    """)
    query_params.append(limit)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("".join(query_parts), query_params)
            rows = cur.fetchall()

    return format_response_object(device_id, rows, normalized_metrics)


def fetch_device_history(table_name, device_id, params):
    metrics = params.resolved_metrics
    end_time = params.resolved_end_time or datetime.now()
    start_time = params.resolved_start_time or (end_time - timedelta(days=1))
    bucket_interval = params.resolved_bucket_interval or "1 minute"

    query_parts = [f"""
        WITH aggregated AS (
            SELECT
                device_id,
                metric,
                AVG(value) AS value,
                unit,
                date_bin(
                    %s::interval,
                    event_time,
                    TIMESTAMP '2001-01-01 00:00:00'
                ) AS bucket_time
            FROM {table_name}
            WHERE device_id = %s
    """]
    query_params = [bucket_interval, device_id]

    if start_time and end_time:
        query_parts.append(" AND event_time >= %s")
        query_params.append(start_time)
        query_parts.append(" AND event_time < %s")
        query_params.append(end_time)

    if metrics:
        query_parts.append(" AND metric = ANY(%s)")
        query_params.append(metrics)

    if params.start is not None and params.end is not None:
        query_parts.append("""
                GROUP BY device_id, metric, unit, bucket_time
            )
            SELECT device_id, metric, value, unit, bucket_time AS event_time
            FROM aggregated
            ORDER BY bucket_time DESC, metric ASC;
        """)
    else:
        query_parts.append("""
                GROUP BY device_id, metric, unit, bucket_time
            ),
            selected_times AS (
                SELECT DISTINCT bucket_time
                FROM aggregated
                ORDER BY bucket_time DESC
                LIMIT %s
            )
            SELECT device_id, metric, value, unit, bucket_time AS event_time
            FROM aggregated
            WHERE bucket_time IN (SELECT bucket_time FROM selected_times)
            ORDER BY bucket_time DESC, metric ASC;
        """)
        query_params.append(params.limit)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("".join(query_parts), query_params)
            rows = cur.fetchall()

    return format_response_object(device_id, rows, metrics)


def resolve_upat_rollup_table(params):
    if params.aggregate != "avg" or params.resolved_bucket_unit is None:
        return None

    bucket_unit = params.resolved_bucket_unit
    bucket_size = params.resolved_bucket_size

    if bucket_unit == "minute":
        if bucket_size % 60 == 0:
            return "upat_measurements_hourly"
        if bucket_size >= 5 and bucket_size % 5 == 0:
            return "upat_measurements_5min"
        return None

    if bucket_unit in {"hour", "day"}:
        return "upat_measurements_hourly"

    return None


def fetch_upat_rollup_history(table_name, device_id, params):
    metrics = params.resolved_metrics
    end_time = params.resolved_end_time or datetime.now()
    start_time = params.resolved_start_time or (end_time - timedelta(days=1))
    bucket_interval = params.resolved_bucket_interval
    source_bucket_interval = (
        "5 minutes"
        if table_name == "upat_measurements_5min"
        else "1 hour"
    )

    query_parts = [f"""
        WITH rollup_state AS (
            SELECT last_measurement_id
            FROM upat_rollup_state
            WHERE pipeline_name = 'upat'
        ),
        raw_source AS (
            SELECT
                measurement.device_id,
                measurement.metric,
                MAX(measurement.unit) AS unit,
                SUM(measurement.value) AS value_sum,
                COUNT(*)::BIGINT AS sample_count,
                date_bin(
                    %s::interval,
                    measurement.event_time,
                    TIMESTAMP '2001-01-01 00:00:00'
                ) AS source_bucket
            FROM upat_measurements AS measurement
            CROSS JOIN rollup_state AS state
            WHERE measurement.id > state.last_measurement_id
              AND measurement.device_id = %s
              AND measurement.event_time >= %s
              AND measurement.event_time < %s
    """]
    query_params = [
        source_bucket_interval,
        device_id,
        start_time,
        end_time,
    ]

    if metrics:
        query_parts.append(" AND measurement.metric = ANY(%s)")
        query_params.append(metrics)

    query_parts.append(f"""
            GROUP BY
                measurement.device_id,
                measurement.metric,
                source_bucket
        ),
        rollup_source AS (
            SELECT
                rollup.device_id,
                rollup.metric,
                rollup.unit,
                rollup.value_avg * rollup.sample_count AS value_sum,
                rollup.sample_count::BIGINT AS sample_count,
                rollup.bucket_start AS source_bucket
            FROM {table_name} AS rollup
            WHERE rollup.device_id = %s
              AND rollup.bucket_start >= %s
              AND rollup.bucket_start < %s
    """)
    query_params.extend([device_id, start_time, end_time])

    if metrics:
        query_parts.append(" AND rollup.metric = ANY(%s)")
        query_params.append(metrics)

    query_parts.append("""
        ),
        combined_source AS (
            SELECT device_id, metric, unit, value_sum, sample_count, source_bucket
            FROM raw_source
            UNION ALL
            SELECT device_id, metric, unit, value_sum, sample_count, source_bucket
            FROM rollup_source
        ),
        aggregated AS (
            SELECT
                device_id,
                metric,
                SUM(value_sum) / NULLIF(SUM(sample_count), 0) AS value,
                MAX(unit) AS unit,
                date_bin(
                    %s::interval,
                    source_bucket,
                    TIMESTAMP '2001-01-01 00:00:00'
                ) AS bucket_time
            FROM combined_source
            GROUP BY device_id, metric, bucket_time
        )
    """)
    query_params.append(bucket_interval)

    if params.start is not None and params.end is not None:
        query_parts.append("""
            SELECT device_id, metric, value, unit, bucket_time AS event_time
            FROM aggregated
            ORDER BY bucket_time DESC, metric ASC;
        """)
    else:
        query_parts.append("""
            , selected_times AS (
                SELECT DISTINCT bucket_time
                FROM aggregated
                ORDER BY bucket_time DESC
                LIMIT %s
            )
            SELECT device_id, metric, value, unit, bucket_time AS event_time
            FROM aggregated
            WHERE bucket_time IN (SELECT bucket_time FROM selected_times)
            ORDER BY bucket_time DESC, metric ASC;
        """)
        query_params.append(params.limit)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("".join(query_parts), query_params)
            rows = cur.fetchall()

    return format_response_object(device_id, rows, metrics)


def fetch_upat_device_history(device_id, params):
    rollup_table = resolve_upat_rollup_table(params)

    if rollup_table:
        return fetch_upat_rollup_history(rollup_table, device_id, params)

    return fetch_device_history("upat_measurements", device_id, params)


def _safe_percent(used, total):
    if not total or total <= 0:
        return None
    return round(max(0.0, min(float(used) / float(total) * 100.0, 100.0)), 1)


def _read_runtime_memory():
    """Return cgroup-aware memory counters without exposing host details."""
    try:
        with open("/sys/fs/cgroup/memory.current", encoding="utf-8") as current_file:
            current = int(current_file.read().strip())
        with open("/sys/fs/cgroup/memory.max", encoding="utf-8") as maximum_file:
            maximum_value = maximum_file.read().strip()
        if maximum_value != "max":
            maximum = int(maximum_value)
            if maximum > 0:
                return current, maximum
    except (OSError, ValueError):
        pass

    try:
        values = {}
        with open("/proc/meminfo", encoding="utf-8") as meminfo:
            for line in meminfo:
                key, value = line.split(":", 1)
                values[key] = int(value.strip().split()[0]) * 1024
        total = values.get("MemTotal")
        available = values.get("MemAvailable")
        if total and available is not None:
            return total - available, total
    except (OSError, ValueError, IndexError):
        pass

    return None, None


def get_runtime_metrics():
    """Collect coarse runtime metrics using only local, sanitized counters."""
    cpu_count = os.cpu_count() or 1
    try:
        cpu_load_percent = round(
            max(0.0, min(os.getloadavg()[0] / cpu_count * 100.0, 100.0)),
            1,
        )
    except (AttributeError, OSError):
        cpu_load_percent = None

    memory_used, memory_total = _read_runtime_memory()

    try:
        disk = shutil.disk_usage("/")
        disk_used_percent = _safe_percent(disk.used, disk.total)
    except OSError:
        disk_used_percent = None

    try:
        with open("/proc/uptime", encoding="utf-8") as uptime_file:
            uptime_seconds = int(float(uptime_file.read().split()[0]))
    except (OSError, ValueError, IndexError):
        uptime_seconds = None

    return {
        "cpu_load_1m_percent": cpu_load_percent,
        "memory_used_percent": _safe_percent(memory_used, memory_total),
        "disk_used_percent": disk_used_percent,
        "uptime_seconds": uptime_seconds,
    }


def fetch_operational_telemetry():
    """Return one indexed fleet aggregate plus coarse database/runtime metrics."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH device_last_readings AS (
                    SELECT
                        'upat' AS device_type,
                        devices.device_id,
                        latest.event_time AS last_reading_at
                    FROM (SELECT DISTINCT device_id FROM upat_devices) AS devices
                    LEFT JOIN LATERAL (
                        SELECT messages.event_time
                        FROM upat_raw_messages AS messages
                        WHERE messages.device_id = devices.device_id
                          AND messages.event_time IS NOT NULL
                        ORDER BY messages.event_time DESC
                        LIMIT 1
                    ) AS latest ON TRUE

                    UNION ALL

                    SELECT
                        'shelly' AS device_type,
                        devices.device_id,
                        latest.event_time AS last_reading_at
                    FROM (SELECT DISTINCT device_id FROM shelly_devices) AS devices
                    LEFT JOIN LATERAL (
                        SELECT messages.event_time
                        FROM shelly_raw_messages AS messages
                        WHERE messages.device_id = devices.device_id
                          AND messages.event_time IS NOT NULL
                        ORDER BY messages.event_time DESC
                        LIMIT 1
                    ) AS latest ON TRUE
                )
                SELECT
                    COUNT(*)::integer AS total_devices,
                    COUNT(*) FILTER (
                        WHERE last_reading_at >=
                            (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
                            - INTERVAL '15 minutes'
                    )::integer AS live_devices,
                    COUNT(*) FILTER (
                        WHERE last_reading_at <
                                (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
                                - INTERVAL '15 minutes'
                          AND last_reading_at >=
                                (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
                                - INTERVAL '24 hours'
                    )::integer AS stale_devices,
                    COUNT(*) FILTER (
                        WHERE last_reading_at <
                                (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
                                - INTERVAL '24 hours'
                           OR last_reading_at IS NULL
                    )::integer AS offline_devices,
                    COUNT(*) FILTER (WHERE device_type = 'upat')::integer AS upat_devices,
                    COUNT(*) FILTER (WHERE device_type = 'shelly')::integer AS shelly_devices,
                    MAX(last_reading_at) AS latest_reading_at,
                    pg_database_size(current_database())::bigint AS database_size_bytes,
                    (
                        SELECT COUNT(*)::integer
                        FROM pg_stat_activity
                        WHERE datname = current_database()
                    ) AS active_connections,
                    current_setting('max_connections')::integer AS max_connections
                FROM device_last_readings;
                """
            )
            row = cur.fetchone()

    total_devices = int(row["total_devices"] or 0)
    live_devices = int(row["live_devices"] or 0)
    active_connections = int(row["active_connections"] or 0)
    max_connections = int(row["max_connections"] or 0)
    runtime_metrics = get_runtime_metrics()

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
            "+00:00",
            "Z",
        ),
        "fleet": {
            "total_devices": total_devices,
            "live_devices": live_devices,
            "stale_devices": int(row["stale_devices"] or 0),
            "offline_devices": int(row["offline_devices"] or 0),
            "upat_devices": int(row["upat_devices"] or 0),
            "shelly_devices": int(row["shelly_devices"] or 0),
            "availability_percent": _safe_percent(live_devices, total_devices),
            "latest_reading_at": row["latest_reading_at"],
            "live_threshold_minutes": 15,
            "offline_threshold_hours": 24,
        },
        "infrastructure": {
            "cpu_load_1m_percent": runtime_metrics.get("cpu_load_1m_percent"),
            "memory_used_percent": runtime_metrics.get("memory_used_percent"),
            "disk_used_percent": runtime_metrics.get("disk_used_percent"),
            "uptime_seconds": runtime_metrics.get("uptime_seconds"),
        },
        "database": {
            "size_bytes": int(row["database_size_bytes"] or 0),
            "active_connections": active_connections,
            "max_connections": max_connections,
            "connections_used_percent": _safe_percent(
                active_connections,
                max_connections,
            ),
        },
    }


@app.get("/health")
def health():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "details": str(e)}


def require_ops_telemetry_token(
    authorization: Annotated[
        str | None,
        Header(alias="Authorization"),
    ] = None,
):
    if len(OPS_TELEMETRY_TOKEN) < 32:
        raise HTTPException(
            status_code=503,
            detail="Operational telemetry authentication is not configured",
        )

    scheme, separator, credentials = (authorization or "").partition(" ")
    if (
        not separator
        or scheme.lower() != "bearer"
        or not hmac.compare_digest(credentials, OPS_TELEMETRY_TOKEN)
    ):
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )


@app.get(
    "/ops/telemetry",
    dependencies=[Depends(require_ops_telemetry_token)],
)
def operational_telemetry():
    try:
        return fetch_operational_telemetry()
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="Operational telemetry is temporarily unavailable",
        ) from None


@app.get("/upat/devices")
def get_all_upat_devices():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, source, device_id, dev_eui, name, created_at
                FROM upat_devices
                ORDER BY source, device_id;
                """
            )
            return cur.fetchall()


@app.get("/shelly/devices")
def get_all_shelly_devices():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, device_id, name, created_at
                FROM shelly_devices
                ORDER BY device_id;
                """
            )
            return cur.fetchall()


@app.get("/simulations/recordings/latest")
def get_latest_simulation_recordings(
    school_id: str = Query(...),
):
    normalized_school_id = normalize_required_text(school_id, "school_id")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    school_id,
                    request_url,
                    request_path,
                    http_status,
                    started_at,
                    completed_at,
                    created_at
                FROM simulation_runs
                WHERE school_id = %s
                  AND success = TRUE
                ORDER BY started_at DESC
                LIMIT 1;
                """,
                (normalized_school_id,),
            )
            run = cur.fetchone()

            if not run:
                raise HTTPException(
                    status_code=404,
                    detail=f"No successful simulation recordings found for school_id={normalized_school_id}",
                )

            cur.execute(
                """
                SELECT
                    id,
                    run_id,
                    school_id,
                    recording_date,
                    room_id,
                    label,
                    physical_instance_count,
                    idf_file,
                    zone_name,
                    thermostat_type,
                    supports_cooling_setpoint,
                    default_occupancy,
                    default_heating_setpoint,
                    default_cooling_setpoint,
                    default_lighting_w_per_m2,
                    default_infiltration_ach,
                    raw_item,
                    created_at,
                    updated_at
                FROM simulation_room_recordings
                WHERE run_id = %s
                ORDER BY room_id ASC;
                """,
                (run["id"],),
            )
            rows = cur.fetchall()

    return {
        "school_id": normalized_school_id,
        "run": {
            "id": run["id"],
            "request_url": run["request_url"],
            "request_path": run["request_path"],
            "http_status": run["http_status"],
            "started_at": run["started_at"],
            "completed_at": run["completed_at"],
            "created_at": run["created_at"],
        },
        "recording_date": rows[0]["recording_date"] if rows else None,
        "count": len(rows),
        "items": [format_simulation_recording(row) for row in rows],
    }


@app.get("/simulations/day-ahead/latest")
def get_latest_day_ahead_simulation_results(
    school_id: str = Query(...),
):
    normalized_school_id = normalize_required_text(school_id, "school_id")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    school_id,
                    recording_date,
                    request_url,
                    request_path,
                    request_body,
                    http_status,
                    status,
                    simulation_engine,
                    external_run_id,
                    day_ahead_date,
                    requested_rooms,
                    successful_rooms,
                    failed_rooms,
                    facility_kwh,
                    equipment_kwh,
                    lighting_kwh,
                    heating_liters,
                    cooling_kwh,
                    fans_hvac_kwh,
                    response_json,
                    started_at,
                    completed_at,
                    created_at,
                    updated_at
                FROM simulation_day_ahead_runs
                WHERE school_id = %s
                  AND success = TRUE
                ORDER BY started_at DESC
                LIMIT 1;
                """,
                (normalized_school_id,),
            )
            run = cur.fetchone()

            if not run:
                raise HTTPException(
                    status_code=404,
                    detail=f"No day-ahead simulation results found for school_id={normalized_school_id}",
                )

            cur.execute(
                """
                SELECT
                    room_id,
                    room_label,
                    status,
                    error_text,
                    average_air_temperature_c,
                    thermal_discomfort_hours,
                    facility_kwh,
                    equipment_kwh,
                    lighting_kwh,
                    heating_liters,
                    cooling_kwh,
                    fans_hvac_kwh
                FROM simulation_day_ahead_room_results
                WHERE run_id = %s
                ORDER BY room_id ASC;
                """,
                (run["id"],),
            )
            rows = cur.fetchall()

    response_json = run.get("response_json")
    hourly_load = (
        response_json.get("hourly_load")
        if isinstance(response_json, dict)
        and isinstance(response_json.get("hourly_load"), dict)
        else None
    )

    return {
        "status": run["status"],
        "simulation_engine": run["simulation_engine"],
        "run_id": run["external_run_id"],
        "school_id": run["school_id"],
        "summary": {
            "requested_rooms": run["requested_rooms"],
            "successful_rooms": run["successful_rooms"],
            "failed_rooms": run["failed_rooms"],
        },
        "day_ahead_date": run["day_ahead_date"],
        "hourly_load": hourly_load,
        "school_totals": {
            "facility_kwh": numeric_or_none(run["facility_kwh"]),
            "equipment_kwh": numeric_or_none(run["equipment_kwh"]),
            "lighting_kwh": numeric_or_none(run["lighting_kwh"]),
            "heating_liters": numeric_or_none(run["heating_liters"]),
            "cooling_kwh": numeric_or_none(run["cooling_kwh"]),
            "fans_hvac_kwh": numeric_or_none(run["fans_hvac_kwh"]),
        },
        "room_results": [format_day_ahead_room_result(row) for row in rows],
        "recording": {
            "id": run["id"],
            "recording_date": run["recording_date"],
            "request_url": run["request_url"],
            "request_path": run["request_path"],
            "request_body": run["request_body"],
            "http_status": run["http_status"],
            "started_at": run["started_at"],
            "completed_at": run["completed_at"],
            "created_at": run["created_at"],
            "updated_at": run["updated_at"],
        },
    }


@app.get("/pv/day-ahead/latest")
def get_latest_pv_day_ahead_forecast():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    forecast_date,
                    daily_energy_kwh,
                    source,
                    model_artifact,
                    features_artifact,
                    started_at,
                    completed_at,
                    created_at,
                    updated_at
                FROM pv_day_ahead_forecast_runs
                WHERE success = TRUE
                ORDER BY started_at DESC
                LIMIT 1;
                """
            )
            run = cur.fetchone()

            if not run:
                raise HTTPException(
                    status_code=404,
                    detail="No PV day-ahead forecast found",
                )

            cur.execute(
                """
                SELECT
                    forecast_timestamp,
                    forecast_hour,
                    predicted_power_kw
                FROM pv_day_ahead_forecast_hourly
                WHERE run_id = %s
                ORDER BY forecast_timestamp ASC;
                """,
                (run["id"],),
            )
            rows = cur.fetchall()

    return {
        "run_id": run["id"],
        "forecast_date": run["forecast_date"],
        "daily_energy_kwh": numeric_or_none(run["daily_energy_kwh"]),
        "source": run["source"],
        "model_artifact": run["model_artifact"],
        "features_artifact": run["features_artifact"],
        "count": len(rows),
        "items": [format_pv_forecast_hour(row) for row in rows],
        "recording": {
            "started_at": run["started_at"],
            "completed_at": run["completed_at"],
            "created_at": run["created_at"],
            "updated_at": run["updated_at"],
        },
    }


@app.get("/weather/hourly/forecast")
def get_weather_hourly_forecast(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
):
    start_time, end_time = resolve_weather_time_bounds(start, end)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    source,
                    latitude,
                    longitude,
                    timezone,
                    forecast_timestamp,
                    forecast_date,
                    forecast_hour,
                    temperature_2m AS temperature_2m_c,
                    dew_point_2m AS dew_point_2m_c,
                    relative_humidity_2m AS relative_humidity_2m_percent,
                    surface_pressure AS surface_pressure_hpa,
                    shortwave_radiation AS shortwave_radiation_w_m2,
                    direct_normal_irradiance AS direct_normal_irradiance_w_m2,
                    diffuse_radiation AS diffuse_radiation_w_m2,
                    wind_direction_10m AS wind_direction_10m_degrees,
                    wind_speed_10m AS wind_speed_10m_ms,
                    weather_code,
                    snow_depth AS snow_depth_m,
                    precipitation AS precipitation_mm,
                    cloud_cover AS cloud_cover_percent,
                    fetched_at
                FROM weather_hourly_forecasts
                WHERE forecast_timestamp >= %s
                  AND forecast_timestamp <= %s
                ORDER BY forecast_timestamp ASC;
                """,
                (start_time, end_time),
            )
            rows = cur.fetchall()

    return {
        "start": start_time,
        "end": end_time,
        "count": len(rows),
        "items": [format_weather_forecast_hour(row) for row in rows],
    }


@app.get("/weather/hourly/latest")
def get_latest_weather_forecast_hour():
    local_now = datetime.now(WEATHER_LOCAL_TZ).replace(
        minute=0,
        second=0,
        microsecond=0,
        tzinfo=None,
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    source,
                    latitude,
                    longitude,
                    timezone,
                    forecast_timestamp,
                    forecast_date,
                    forecast_hour,
                    temperature_2m AS temperature_2m_c,
                    dew_point_2m AS dew_point_2m_c,
                    relative_humidity_2m AS relative_humidity_2m_percent,
                    surface_pressure AS surface_pressure_hpa,
                    shortwave_radiation AS shortwave_radiation_w_m2,
                    direct_normal_irradiance AS direct_normal_irradiance_w_m2,
                    diffuse_radiation AS diffuse_radiation_w_m2,
                    wind_direction_10m AS wind_direction_10m_degrees,
                    wind_speed_10m AS wind_speed_10m_ms,
                    weather_code,
                    snow_depth AS snow_depth_m,
                    precipitation AS precipitation_mm,
                    cloud_cover AS cloud_cover_percent,
                    fetched_at
                FROM weather_hourly_forecasts
                WHERE forecast_timestamp >= %s
                ORDER BY forecast_timestamp ASC
                LIMIT 1;
                """,
                (local_now,),
            )
            row = cur.fetchone()

    if not row:
        raise HTTPException(
            status_code=404,
            detail="No current or future hourly weather forecast found",
        )

    return format_weather_forecast_hour(row)


@app.get("/upat/device/{device_id}/latest")
def get_latest_measurements(
    device_id: str,
    metric: list[str] | None = Query(default=None),
    limit: int = Query(default=30, le=1000),
):
    return fetch_device_latest("upat_measurements", device_id, metric, limit)


@app.get("/shelly/device/{device_id}/latest")
def get_latest_shelly_measurements(
    device_id: str,
    metric: list[str] | None = Query(default=None),
    limit: int = Query(default=30, le=1000),
):
    return fetch_device_latest("shelly_measurements", device_id, metric, limit)


@app.get("/upat/device/{device_id}/history")
def get_device_history(
    device_id: str,
    params: Annotated[HistoryQueryParams, Query()],
):
    return fetch_upat_device_history(device_id, params)


@app.get("/shelly/device/{device_id}/history")
def get_shelly_device_history(
    device_id: str,
    params: Annotated[HistoryQueryParams, Query()],
):
    return fetch_device_history("shelly_measurements", device_id, params)


@app.get("/shelly/hourly-energy")
def get_shelly_hourly_energy(
    device_id: list[str] | None = Query(default=None),
    start: str | None = None,
    end: str | None = None,
    working_only: bool = Query(default=False),
):
    device_ids = normalize_device_ids(device_id)

    if not device_ids:
        raise HTTPException(
            status_code=400,
            detail="At least one device_id must be provided",
        )

    start_time, end_time = resolve_energy_time_bounds(start, end)
    plug_ids, pro3em_ids = split_shelly_device_ids(device_ids)

    items = []

    with get_connection() as conn:
        with conn.cursor() as cur:
            if plug_ids:
                cur.execute(
                    """
                    SELECT
                        device_id,
                        window_start,
                        window_end,
                        energy_wh,
                        is_working_day,
                        is_working_hour,
                        created_at
                    FROM shelly_plug_hourly_energy
                    WHERE device_id = ANY(%s)
                      AND window_start >= %s
                      AND window_end <= %s
                      AND (%s = FALSE OR (is_working_day = 1 AND is_working_hour = 1))
                    ORDER BY window_start DESC, device_id ASC;
                    """,
                    (plug_ids, start_time, end_time, working_only),
                )
                rows = cur.fetchall()

                for row in rows:
                    items.append({
                        "device_id": row["device_id"],
                        "device_type": "plug",
                        "window_start": row["window_start"],
                        "window_end": row["window_end"],
                        "is_working_day": row["is_working_day"],
                        "is_working_hour": row["is_working_hour"],
                        "energy_wh": {
                            "total": round(float(row["energy_wh"] or 0.0), 3),
                        },
                        "created_at": row["created_at"],
                    })

            if pro3em_ids:
                cur.execute(
                    """
                    SELECT
                        device_id,
                        window_start,
                        window_end,
                        a_energy_wh,
                        b_energy_wh,
                        c_energy_wh,
                        total_energy_wh,
                        is_working_day,
                        is_working_hour,
                        created_at
                    FROM shelly_pro3em_hourly_energy
                    WHERE device_id = ANY(%s)
                      AND window_start >= %s
                      AND window_end <= %s
                      AND (%s = FALSE OR (is_working_day = 1 AND is_working_hour = 1))
                    ORDER BY window_start DESC, device_id ASC;
                    """,
                    (pro3em_ids, start_time, end_time, working_only),
                )
                rows = cur.fetchall()

                for row in rows:
                    items.append({
                        "device_id": row["device_id"],
                        "device_type": "pro3em",
                        "window_start": row["window_start"],
                        "window_end": row["window_end"],
                        "is_working_day": row["is_working_day"],
                        "is_working_hour": row["is_working_hour"],
                        "energy_wh": {
                            "a": round(float(row["a_energy_wh"] or 0.0), 3),
                            "b": round(float(row["b_energy_wh"] or 0.0), 3),
                            "c": round(float(row["c_energy_wh"] or 0.0), 3),
                            "total": round(float(row["total_energy_wh"] or 0.0), 3),
                        },
                        "created_at": row["created_at"],
                    })

    return {
        "device_ids": device_ids,
        "start": start_time,
        "end": end_time,
        "working_only": working_only,
        "count": len(items),
        "items": items,
    }


@app.get("/shelly/energy")
def get_shelly_energy(
    device_id: list[str] | None = Query(default=None),
    start: str | None = None,
    end: str | None = None,
    working_only: bool = Query(default=False),
):
    device_ids = normalize_device_ids(device_id)

    if not device_ids:
        raise HTTPException(
            status_code=400,
            detail="At least one device_id must be provided",
        )

    start_time, end_time = resolve_energy_time_bounds(start, end)
    plug_ids, pro3em_ids = split_shelly_device_ids(device_ids)

    results = []

    with get_connection() as conn:
        with conn.cursor() as cur:
            if plug_ids:
                cur.execute(
                    """
                    SELECT
                        device_id,
                        COALESCE(SUM(energy_wh), 0) AS total_wh
                    FROM shelly_plug_hourly_energy
                    WHERE device_id = ANY(%s)
                      AND window_start >= %s
                      AND window_end <= %s
                      AND (%s = FALSE OR (is_working_day = 1 AND is_working_hour = 1))
                    GROUP BY device_id
                    ORDER BY device_id ASC;
                    """,
                    (plug_ids, start_time, end_time, working_only),
                )
                rows = cur.fetchall()

                for row in rows:
                    results.append({
                        "device_id": row["device_id"],
                        "device_type": "plug",
                        "energy_wh": {
                            "total": round(float(row["total_wh"] or 0.0), 3),
                        },
                    })

            if pro3em_ids:
                cur.execute(
                    """
                    SELECT
                        device_id,
                        COALESCE(SUM(a_energy_wh), 0) AS a_wh,
                        COALESCE(SUM(b_energy_wh), 0) AS b_wh,
                        COALESCE(SUM(c_energy_wh), 0) AS c_wh,
                        COALESCE(SUM(total_energy_wh), 0) AS total_wh
                    FROM shelly_pro3em_hourly_energy
                    WHERE device_id = ANY(%s)
                      AND window_start >= %s
                      AND window_end <= %s
                      AND (%s = FALSE OR (is_working_day = 1 AND is_working_hour = 1))
                    GROUP BY device_id
                    ORDER BY device_id ASC;
                    """,
                    (pro3em_ids, start_time, end_time, working_only),
                )
                rows = cur.fetchall()

                for row in rows:
                    results.append({
                        "device_id": row["device_id"],
                        "device_type": "pro3em",
                        "energy_wh": {
                            "a": round(float(row["a_wh"] or 0.0), 3),
                            "b": round(float(row["b_wh"] or 0.0), 3),
                            "c": round(float(row["c_wh"] or 0.0), 3),
                            "total": round(float(row["total_wh"] or 0.0), 3),
                        },
                    })

    return {
        "device_ids": device_ids,
        "start": start_time,
        "end": end_time,
        "working_only": working_only,
        "count": len(results),
        "items": results,
    }


@app.get("/shelly/device/{device_id}/hourly-energy")
def get_shelly_device_hourly_energy_history(
    device_id: str,
    start: str | None = None,
    end: str | None = None,
    working_only: bool = Query(default=False),
):
    start_time, end_time = resolve_energy_time_bounds(start, end)
    spec = get_shelly_device_db_table(device_id)

    with get_connection() as conn:
        with conn.cursor() as cur:
            if spec["device_type"] == "plug":
                cur.execute(
                    """
                    SELECT
                        device_id,
                        window_start,
                        window_end,
                        energy_wh,
                        is_working_day,
                        is_working_hour,
                        created_at
                    FROM shelly_plug_hourly_energy
                    WHERE device_id = %s
                      AND window_start >= %s
                      AND window_end <= %s
                      AND (%s = FALSE OR (is_working_day = 1 AND is_working_hour = 1))
                    ORDER BY window_start DESC;
                    """,
                    (device_id, start_time, end_time, working_only),
                )
                rows = cur.fetchall()

                return {
                    "device_id": device_id,
                    "device_type": "plug",
                    "start": start_time,
                    "end": end_time,
                    "working_only": working_only,
                    "count": len(rows),
                    "items": [
                        {
                            "window_start": row["window_start"],
                            "window_end": row["window_end"],
                            "is_working_day": row["is_working_day"],
                            "is_working_hour": row["is_working_hour"],
                            "energy_wh": {
                                "total": round(float(row["energy_wh"] or 0.0), 3),
                            },
                            "created_at": row["created_at"],
                        }
                        for row in rows
                    ],
                }

            cur.execute(
                """
                SELECT
                    device_id,
                    window_start,
                    window_end,
                    a_energy_wh,
                    b_energy_wh,
                    c_energy_wh,
                    total_energy_wh,
                    is_working_day,
                    is_working_hour,
                    created_at
                FROM shelly_pro3em_hourly_energy
                WHERE device_id = %s
                  AND window_start >= %s
                  AND window_end <= %s
                  AND (%s = FALSE OR (is_working_day = 1 AND is_working_hour = 1))
                ORDER BY window_start DESC;
                """,
                (device_id, start_time, end_time, working_only),
            )
            rows = cur.fetchall()

    return {
        "device_id": device_id,
        "device_type": "pro3em",
        "start": start_time,
        "end": end_time,
        "working_only": working_only,
        "count": len(rows),
        "items": [
            {
                "window_start": row["window_start"],
                "window_end": row["window_end"],
                "is_working_day": row["is_working_day"],
                "is_working_hour": row["is_working_hour"],
                "energy_wh": {
                    "a": round(float(row["a_energy_wh"] or 0.0), 3),
                    "b": round(float(row["b_energy_wh"] or 0.0), 3),
                    "c": round(float(row["c_energy_wh"] or 0.0), 3),
                    "total": round(float(row["total_energy_wh"] or 0.0), 3),
                },
                "created_at": row["created_at"],
            }
            for row in rows
        ],
    }


@app.get("/shelly/device/{device_id}/energy")
def get_shelly_device_energy(
    device_id: str,
    start: str | None = None,
    end: str | None = None,
    working_only: bool = Query(default=False),
):
    start_time, end_time = resolve_energy_time_bounds(start, end)
    spec = get_shelly_device_db_table(device_id)

    with get_connection() as conn:
        with conn.cursor() as cur:
            if spec["device_type"] == "plug":
                cur.execute(
                    """
                    SELECT COALESCE(SUM(energy_wh), 0) AS total_wh
                    FROM shelly_plug_hourly_energy
                    WHERE device_id = %s
                      AND window_start >= %s
                      AND window_end <= %s
                      AND (%s = FALSE OR (is_working_day = 1 AND is_working_hour = 1));
                    """,
                    (device_id, start_time, end_time, working_only),
                )
                row = cur.fetchone()

                total = round(float(row["total_wh"] or 0.0), 3)

                return {
                    "device_id": device_id,
                    "device_type": "plug",
                    "start": start_time,
                    "end": end_time,
                    "working_only": working_only,
                    "energy_wh": {
                        "total": total,
                    },
                }

            cur.execute(
                """
                SELECT
                    COALESCE(SUM(a_energy_wh), 0) AS a_wh,
                    COALESCE(SUM(b_energy_wh), 0) AS b_wh,
                    COALESCE(SUM(c_energy_wh), 0) AS c_wh,
                    COALESCE(SUM(total_energy_wh), 0) AS total_wh
                FROM shelly_pro3em_hourly_energy
                WHERE device_id = %s
                  AND window_start >= %s
                  AND window_end <= %s
                  AND (%s = FALSE OR (is_working_day = 1 AND is_working_hour = 1));
                """,
                (device_id, start_time, end_time, working_only),
            )
            row = cur.fetchone()

    a = round(float(row["a_wh"] or 0.0), 3)
    b = round(float(row["b_wh"] or 0.0), 3)
    c = round(float(row["c_wh"] or 0.0), 3)
    total = round(float(row["total_wh"] or 0.0), 3)

    return {
        "device_id": device_id,
        "device_type": "pro3em",
        "start": start_time,
        "end": end_time,
        "working_only": working_only,
        "energy_wh": {
            "a": a,
            "b": b,
            "c": c,
            "total": total,
        },
    }
