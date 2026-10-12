import hmac
import os
import shutil
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Annotated
from zoneinfo import ZoneInfo

from database import get_connection
from auth_service import AuthVerifyRateLimiter, build_auth_router
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from readers.measurements import fetch_device_latest, format_response_object
from readers.shelly_storage import source_and_average
from readers.shelly_energy import fetch_shelly_hourly_energy_rows
from schemas import HistoryQueryParams, history_query_params, parse_datetime_bound

app = FastAPI()


@app.exception_handler(RequestValidationError)
async def redact_auth_request_validation_error(
    request: Request,
    exc: RequestValidationError,
):
    if request.url.path.startswith(("/internal/auth/", "/auth/")):
        return JSONResponse(
            status_code=422,
            content={"detail": "Invalid authentication request."},
            headers={"Cache-Control": "no-store"},
        )

    return await request_validation_exception_handler(request, exc)


OPS_TELEMETRY_TOKEN = os.getenv("OPS_TELEMETRY_TOKEN", "").strip()
AUTH_SERVICE_TOKEN = os.getenv("AUTH_SERVICE_TOKEN", "").strip()
AUTH_VERIFY_RATE_LIMITER = AuthVerifyRateLimiter()
WEATHER_TIMEZONE = os.getenv("OPEN_METEO_TIMEZONE", "Europe/Athens")
WEATHER_LOCAL_TZ = ZoneInfo(WEATHER_TIMEZONE)
PV_ACTUALS_MAX_DAYS = 90
PV_ACTUALS_TIMEZONE = "Europe/Athens"
PV_ACTUALS_LOCAL_TZ = ZoneInfo(PV_ACTUALS_TIMEZONE)

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

app.include_router(
    build_auth_router(
        connection_factory=lambda: get_connection(),
        service_token_getter=lambda: AUTH_SERVICE_TOKEN,
        verify_rate_limiter=AUTH_VERIFY_RATE_LIMITER,
    )
)


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


def fetch_device_history(table_name, device_id, params):
    relation, average = source_and_average(table_name)
    metrics = params.resolved_metrics
    end_time = params.resolved_end_time or datetime.now(timezone.utc)
    start_time = params.resolved_start_time or (end_time - timedelta(days=1))
    spec = params.resolved_interval
    bucket_interval = "Europe/Athens" if spec.calendar_day else spec.sql_duration
    bucket_sql = ("date_trunc('day', event_time, %s)" if spec.calendar_day else
                  "date_bin(%s::interval, event_time, TIMESTAMPTZ '2001-01-01 00:00:00+00')")

    query_parts = [f"""
        WITH aggregated AS (
            SELECT
                device_id,
                metric,
                {average} AS value,
                unit,
                {bucket_sql} AS bucket_time
            FROM {relation}
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
    if params.aggregate != "avg":
        return None
    spec = params.resolved_interval
    if spec.calendar_day or spec.minutes % 60 == 0:
        return "upat_measurements_hourly"
    if spec.minutes >= 5 and spec.minutes % 5 == 0:
        return "upat_measurements_5min"
    return None


def fetch_upat_rollup_history(table_name, device_id, params):
    metrics = params.resolved_metrics
    end_time = params.resolved_end_time or datetime.now(timezone.utc)
    start_time = params.resolved_start_time or (end_time - timedelta(days=1))
    spec = params.resolved_interval
    bucket_interval = "Europe/Athens" if spec.calendar_day else spec.sql_duration
    bucket_sql = ("date_trunc('day', source_bucket, %s)" if spec.calendar_day else
                  "date_bin(%s::interval, source_bucket, TIMESTAMPTZ '2001-01-01 00:00:00+00')")
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
                    TIMESTAMPTZ '2001-01-01 00:00:00+00'
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

    query_parts.append(f"""
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
                {bucket_sql} AS bucket_time
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
                            CURRENT_TIMESTAMP
                            - INTERVAL '15 minutes'
                    )::integer AS live_devices,
                    COUNT(*) FILTER (
                        WHERE last_reading_at <
                                CURRENT_TIMESTAMP
                                - INTERVAL '15 minutes'
                          AND last_reading_at >=
                                CURRENT_TIMESTAMP
                                - INTERVAL '24 hours'
                    )::integer AS stale_devices,
                    COUNT(*) FILTER (
                        WHERE last_reading_at <
                                CURRENT_TIMESTAMP
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


def validate_pv_readings_range(start_date: date, end_date: date) -> None:
    if start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail="start_date must be on or before end_date",
        )
    day_count = (end_date - start_date).days + 1
    if day_count > PV_ACTUALS_MAX_DAYS:
        raise HTTPException(
            status_code=400,
            detail=f"Date range exceeds maximum of {PV_ACTUALS_MAX_DAYS} days",
        )
    if end_date == date.max:
        raise HTTPException(
            status_code=400,
            detail="end_date is outside the supported range",
        )


@app.get(
    "/pv/readings",
    dependencies=[Depends(require_ops_telemetry_token)],
)
def get_pv_readings(
    start_date: date = Query(...),
    end_date: date = Query(...),
):
    """Return bounded, read-only five-minute plant metrics for app consumers."""
    validate_pv_readings_range(start_date, end_date)
    range_start_utc = datetime.combine(
        start_date,
        time.min,
        tzinfo=PV_ACTUALS_LOCAL_TZ,
    ).astimezone(timezone.utc)
    range_end_utc = datetime.combine(
        end_date + timedelta(days=1),
        time.min,
        tzinfo=PV_ACTUALS_LOCAL_TZ,
    ).astimezone(timezone.utc)
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        r.observed_at,
                        r.local_date,
                        r.active_power_kw,
                        r.reactive_power_kvar,
                        r.mppt_power_kw,
                        r.power_factor,
                        r.quality_status
                    FROM pv_plant_readings_5m r
                    WHERE r.observed_at >= %s
                      AND r.observed_at < %s
                      AND r.quality_status <> 'invalid'
                    ORDER BY r.observed_at ASC;
                    """,
                    (range_start_utc, range_end_utc),
                )
                rows = cur.fetchall()
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="PV readings are temporarily unavailable",
        ) from None

    points = [
        {
            "observed_at": row["observed_at"],
            "local_date": row["local_date"].isoformat(),
            "active_power_kw": numeric_or_none(row["active_power_kw"]),
            "reactive_power_kvar": numeric_or_none(row["reactive_power_kvar"]),
            "mppt_power_kw": numeric_or_none(row["mppt_power_kw"]),
            "power_factor": numeric_or_none(row["power_factor"]),
            "quality_status": row["quality_status"],
        }
        for row in rows
    ]
    return {
        "source_id": "postgres-pv-plant-readings",
        "timezone": PV_ACTUALS_TIMEZONE,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "count": len(points),
        "points": points,
    }


@app.get(
    "/pv/readings/bounds",
    dependencies=[Depends(require_ops_telemetry_token)],
)
def get_pv_readings_bounds():
    """Return the available local-date bounds for the bounded PV reader."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        MIN(r.local_date) AS min_date,
                        MAX(r.local_date) AS max_date
                    FROM pv_plant_readings_5m r
                    WHERE r.active_power_kw IS NOT NULL
                      AND r.quality_status <> 'invalid';
                    """,
                )
                row = cur.fetchone()
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="PV readings are temporarily unavailable",
        ) from None

    min_date = row["min_date"] if row else None
    max_date = row["max_date"] if row else None
    return {
        "source_id": "postgres-pv-plant-readings",
        "timezone": PV_ACTUALS_TIMEZONE,
        "min_date": min_date.isoformat() if min_date else None,
        "max_date": max_date.isoformat() if max_date else None,
    }


@app.get(
    "/pv/day-ahead/range",
    dependencies=[Depends(require_ops_telemetry_token)],
)
def get_pv_day_ahead_forecast_range(
    start_date: date = Query(...),
    end_date: date = Query(...),
):
    """Return the latest persisted forecast for each bounded local date."""
    validate_pv_readings_range(start_date, end_date)
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH latest_runs AS (
                        SELECT DISTINCT ON (forecast_date)
                            id,
                            forecast_date
                        FROM pv_day_ahead_forecast_runs
                        WHERE success = TRUE
                          AND forecast_date >= %s
                          AND forecast_date <= %s
                        ORDER BY forecast_date ASC, started_at DESC, id DESC
                    )
                    SELECT
                        r.forecast_date,
                        h.forecast_timestamp,
                        h.forecast_hour,
                        h.predicted_power_kw
                    FROM latest_runs r
                    JOIN pv_day_ahead_forecast_hourly h ON h.run_id = r.id
                    ORDER BY r.forecast_date ASC, h.forecast_timestamp ASC;
                    """,
                    (start_date, end_date),
                )
                rows = cur.fetchall()
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="PV forecasts are temporarily unavailable",
        ) from None

    forecasts_by_date: dict[str, dict] = {}
    for row in rows:
        forecast_date = row["forecast_date"].isoformat()
        forecast = forecasts_by_date.setdefault(
            forecast_date,
            {
                "forecast_date": forecast_date,
                "count": 0,
                "items": [],
            },
        )
        forecast["items"].append(
            {
                "timestamp": row["forecast_timestamp"],
                "hour": row["forecast_hour"],
                "predicted_power_kw": numeric_or_none(
                    row["predicted_power_kw"]
                ),
            }
        )
        forecast["count"] += 1

    forecasts = list(forecasts_by_date.values())
    return {
        "source_id": "postgres-pv-day-ahead-forecasts",
        "timezone": PV_ACTUALS_TIMEZONE,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "count": len(forecasts),
        "forecasts": forecasts,
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


@app.get("/upat/device/{device_id}/latest")
def get_latest_measurements(
    device_id: str,
    metric: list[str] | None = Query(default=None),
    limit: int = Query(default=30, le=1000),
):
    return fetch_device_latest("upat_measurements", device_id, metric, limit,
                               connection_factory=get_connection)


@app.get("/shelly/device/{device_id}/latest")
def get_latest_shelly_measurements(
    device_id: str,
    metric: list[str] | None = Query(default=None),
    limit: int = Query(default=30, le=1000),
):
    return fetch_device_latest("shelly_measurements", device_id, metric, limit,
                               connection_factory=get_connection)


@app.get("/upat/device/{device_id}/history")
def get_device_history(
    device_id: str,
    params: Annotated[HistoryQueryParams, Depends(history_query_params)],
):
    return fetch_upat_device_history(device_id, params)


@app.get("/shelly/device/{device_id}/history")
def get_shelly_device_history(
    device_id: str,
    params: Annotated[HistoryQueryParams, Depends(history_query_params)],
):
    return fetch_device_history("shelly_measurements", device_id, params)


@app.get("/shelly/hourly-energy")
def get_shelly_hourly_energy(
    device_id: list[str] | None = Query(default=None),
    start: str | None = None,
    end: str | None = None,
    working_only: bool = Query(default=False),
):
    return fetch_shelly_hourly_energy_rows(
        device_id, start, end, working_only, connection_factory=get_connection
    )


# End-user data/API lives with PostgreSQL; simulation compute is a separate service.
from monitoring.install import install as install_monitoring

def require_session_service_token(authorization: Annotated[str | None, Header(alias="Authorization")] = None):
    scheme, _, credentials = (authorization or "").partition(" ")
    if len(AUTH_SERVICE_TOKEN) < 32:
        raise HTTPException(503, "Authentication service is not configured", headers={"Cache-Control":"no-store"})
    if scheme.lower() != "bearer" or not hmac.compare_digest(credentials, AUTH_SERVICE_TOKEN):
        raise HTTPException(401, "Invalid service credentials", headers={"Cache-Control":"no-store"})

install_monitoring(app, require_session_service_token)

# Additive service access; migrate callers before removing the public port.
from monitoring.service_access import build_data_service_router

app.include_router(build_data_service_router(app.routes))
