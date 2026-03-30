import os
from datetime import datetime, timedelta
from typing import Annotated

from fastapi import FastAPI, Query, HTTPException
import psycopg2
from psycopg2.extras import RealDictCursor

from schemas import HistoryQueryParams, normalize_metrics, parse_datetime_bound

# Start the FastAPI application
app = FastAPI()

# Define database connection parameters
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_INTERNAL_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")


# Round numeric values to 1 decimal place
def round_numeric(value):
    if isinstance(value, (int, float)) and value is not None:
        return round(value, 1)
    return value


# Establish a new PostgreSQL connection
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        cursor_factory=RealDictCursor,
    )


# Define `/health` endpoint to check PostgreSQL connectivity
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


# Define `/upat/devices` endpoint to list all environmental devices in the PostgreSQL database
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


# Define `/shelly/devices` endpoint to list all Shelly devices in the PostgreSQL database
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


# Define `/upat/device/{device_id}/latest` endpoint to fetch the latest measurements for a specific UPAT device
@app.get("/upat/device/{device_id}/latest")
def get_latest_measurements(
        device_id: str,
        metric: list[str] | None = Query(default=None),
        limit: int = Query(default=30, le=1000),
    ):
    return fetch_device_latest("upat_measurements", device_id, metric, limit)


# Define `/shelly/device/{device_id}/latest` endpoint to fetch the latest measurements for a specific Shelly device
@app.get("/shelly/device/{device_id}/latest")
def get_latest_shelly_measurements(
        device_id: str,
        metric: list[str] | None = Query(default=None),
        limit: int = Query(default=30, le=1000),
    ):
    return fetch_device_latest("shelly_measurements", device_id, metric, limit)


# Define `/upat/device/{device_id}/history` endpoint to fetch the historical measurements for a specific UPAT device
@app.get("/upat/device/{device_id}/history")
def get_device_history(
        device_id: str,
        params: Annotated[HistoryQueryParams, Query()],
    ):
    return fetch_device_history("upat_measurements", device_id, params)


# Define `/shelly/device/{device_id}/history` endpoint to fetch the historical measurements for a specific Shelly device
@app.get("/shelly/device/{device_id}/history")
def get_shelly_device_history(
        device_id: str,
        params: Annotated[HistoryQueryParams, Query()],
    ):
    return fetch_device_history("shelly_measurements", device_id, params)


# Group measurements by event_time and format the response object
def format_response_object(device_id, rows, metrics=None):
    snapshots = []
    snapshots_by_time = {}

    # For every PostgreSQL row in the query result, group measurements by event_time
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


# Fetch the latest measurements for a specific device, optionally filtered by metrics and limited by count
def fetch_device_latest(table_name, device_id, metrics, limit):
    # Get a sorted list including the normalized metric names
    normalized_metrics = normalize_metrics(metrics)

    # Initialize query parts and parameters for the SQL query
    query_params = []

    # Incorporate device_id parameter for the SQL query
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

    # Incorporate `metric` parameter if provided for the SQL query
    if normalized_metrics:
        query_parts.append(" AND metric = ANY(%s)")
        query_params.append(normalized_metrics)

    # Incorporate `limit` parameter for the SQL query
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


# Fetch the historical measurements for a specific device, optionally filtered by metrics, time range, and bucket interval
def fetch_device_history(table_name, device_id, params):
    metrics = params.resolved_metrics
    end_time   = params.resolved_end_time or datetime.now()
    start_time = params.resolved_start_time or (end_time - timedelta(days=1))
    bucket_interval = params.resolved_bucket_interval or "1 minute"

    # Incorporate device_id AND bucket_interval parameters for the SQL query
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

    # Incorporate datetime parameters for the SQL query
    if start_time and end_time:
        query_parts.append(" AND event_time >= %s")
        query_params.append(start_time)
        query_parts.append(" AND event_time <= %s")
        query_params.append(end_time)
    # Incorporate metrics parameters for the SQL query
    if metrics:
        query_parts.append(" AND metric = ANY(%s)")
        query_params.append(metrics)

    # If both start and end time are provided, group by the bucket_time and return all results
    if params.start is not None and params.end is not None:
        query_parts.append("""
                GROUP BY device_id, metric, unit, bucket_time
            )
            SELECT device_id, metric, value, unit, bucket_time AS event_time
            FROM aggregated
            ORDER BY bucket_time DESC, metric ASC;
        """)
    # If only start or end time is provided, group by the bucket_time and return only limited results for each metric
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

from fastapi import HTTPException
from schemas import HistoryQueryParams, normalize_metrics, parse_datetime_bound


@app.get("/shelly/device/{device_id}/energy")
def get_shelly_device_energy(
    device_id: str,
    start: str,
    end: str,
):
    start_time = parse_datetime_bound(start, "start")
    end_time = parse_datetime_bound(end, "end")

    if start_time > end_time:
        raise HTTPException(
            status_code=400,
            detail="start must be earlier than or equal to end",
        )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT device_id, metric, value, unit, event_time
                FROM shelly_measurements
                WHERE device_id = %s
                  AND metric = ANY(%s)
                  AND event_time >= %s
                  AND event_time <= %s
                ORDER BY event_time ASC, metric ASC;
                """,
                (
                    device_id,
                    ["a_act_power", "b_act_power", "c_act_power"],
                    start_time,
                    end_time,
                ),
            )
            rows = cur.fetchall()

    by_time = {}
    for row in rows:
        ts = row["event_time"]
        if ts not in by_time:
            by_time[ts] = {}
        by_time[ts][row["metric"]] = row["value"]

    timestamps = sorted(by_time.keys())

    if len(timestamps) < 2:
        return {
            "device_id": device_id,
            "start": start_time,
            "end": end_time,
            "energy_wh": {
                "a": 0.0,
                "b": 0.0,
                "c": 0.0,
                "total": 0.0,
            },
        }

    energy = {"a": 0.0, "b": 0.0, "c": 0.0}

    phase_metric_map = {
        "a": "a_act_power",
        "b": "b_act_power",
        "c": "c_act_power",
    }

    for i in range(1, len(timestamps)):
        t0 = timestamps[i - 1]
        t1 = timestamps[i]
        dt_hours = (t1 - t0).total_seconds() / 3600.0

        prev_vals = by_time[t0]
        curr_vals = by_time[t1]

        for phase, metric in phase_metric_map.items():
            if metric in prev_vals and metric in curr_vals:
                p0 = prev_vals[metric]
                p1 = curr_vals[metric]
                energy[phase] += ((p0 + p1) / 2.0) * dt_hours

    return {
        "device_id": device_id,
        "start": start_time,
        "end": end_time,
        "energy_wh": {
            "a": round(energy["a"], 3),
            "b": round(energy["b"], 3),
            "c": round(energy["c"], 3),
            "total": round(energy["a"] + energy["b"] + energy["c"], 3),
        },
    }