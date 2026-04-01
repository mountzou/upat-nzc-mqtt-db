import os
from datetime import datetime, timedelta
from typing import Annotated

from fastapi import FastAPI, Query, HTTPException
import psycopg2
from psycopg2.extras import RealDictCursor

from schemas import HistoryQueryParams, normalize_metrics, parse_datetime_bound

app = FastAPI()

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_INTERNAL_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")


def round_numeric(value):
    if isinstance(value, (int, float)) and value is not None:
        return round(value, 1)
    return value


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        cursor_factory=RealDictCursor,
    )


def normalize_device_ids(device_ids: list[str] | None):
    if not device_ids:
        return None
    return sorted({d.strip() for d in device_ids if d and d.strip()}) or None


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


def get_shelly_energy_table_spec(device_id: str):
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
    plug_ids = [d for d in device_ids if d.startswith("shellyplug")]
    pro3em_ids = [d for d in device_ids if d.startswith("shellypro3em")]
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
        query_parts.append(" AND event_time <= %s")
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
    return fetch_device_history("upat_measurements", device_id, params)


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
    spec = get_shelly_energy_table_spec(device_id)

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
    spec = get_shelly_energy_table_spec(device_id)

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