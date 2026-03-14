import os
from typing import Annotated

from fastapi import FastAPI, Query
import psycopg2
from psycopg2.extras import RealDictCursor

from schemas import HistoryQueryParams

app = FastAPI()

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_INTERNAL_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        cursor_factory=RealDictCursor,
    )


def round_numeric(value):
    if isinstance(value, (int, float)) and value is not None:
        return round(value, 1)
    return value


def build_measurements_response(device_id, rows, metrics=None):
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


@app.get("/health")
def health():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.fetchone()
        cur.close()
        conn.close()
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "details": str(e)}


@app.get("/devices")
def list_devices():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, source, device_id, dev_eui, name, created_at
        FROM devices
        ORDER BY source, device_id;
        """
    )
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows


@app.get("/devices/{device_id}/latest")
def get_latest_measurements(
    device_id: str,
    metric: str | None = Query(default=None),
    limit: int = Query(default=30, le=1000),
):
    conn = get_connection()
    cur = conn.cursor()
    metrics = None

    if metric:
        metrics = {
            item.strip()
            for item in metric.split(",")
            if item.strip()
        }

    query = """
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
            FROM measurements
            WHERE device_id = %s
    """
    query_params = [device_id]

    if metrics:
        query += " AND metric = ANY(%s)"
        query_params.append(sorted(metrics))

    query += """
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
    """
    query_params.append(limit)
    cur.execute(query, query_params)
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return build_measurements_response(device_id, rows, metrics)


@app.get("/devices/{device_id}/history")
@app.get("/device/{device_id}/history")
def get_device_history(
    device_id: str,
    params: Annotated[HistoryQueryParams, Query()],
):
    conn = get_connection()
    cur = conn.cursor()

    metrics    = params.resolved_metrics
    start_time = params.resolved_start_time
    end_time   = params.resolved_end_time

    has_aggregation = any(
        value is not None
        for value in (params.aggregate, params.bucket, params.bucket_unit, params.bucket_size)
    )

    if has_aggregation:

        query = """
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
                FROM measurements
                WHERE device_id = %s
        """
        query_params = [params.resolved_bucket_interval, device_id]

        if start_time:
            query += " AND event_time >= %s"
            query_params.append(start_time)
        if end_time:
            query += " AND event_time <= %s"
            query_params.append(end_time)

        if metrics:
            query += " AND metric = ANY(%s)"
            query_params.append(sorted(metrics))

        query += """
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
        """
        query_params.append(params.limit)
        cur.execute(query, query_params)
        
    elif metrics:
        query = """
            WITH selected_times AS (
                SELECT DISTINCT event_time
                FROM measurements
                WHERE device_id = %s AND metric = ANY(%s)
        """
        query_params = [device_id, sorted(metrics)]

        if start_time:
            query += " AND event_time >= %s"
            query_params.append(start_time)
        if end_time:
            query += " AND event_time <= %s"
            query_params.append(end_time)

        query += """
                ORDER BY event_time DESC
                LIMIT %s
            )
            SELECT device_id, metric, value, unit, event_time
            FROM measurements
            WHERE device_id = %s
              AND event_time IN (SELECT event_time FROM selected_times)
            ORDER BY event_time DESC, metric ASC;
        """
        query_params.extend([params.limit, device_id])
        cur.execute(query, query_params)
    else:
        query = """
            WITH selected_times AS (
                SELECT DISTINCT event_time
                FROM measurements
                WHERE device_id = %s
        """
        query_params = [device_id]

        if start_time:
            query += " AND event_time >= %s"
            query_params.append(start_time)
        if end_time:
            query += " AND event_time <= %s"
            query_params.append(end_time)

        query += """
                ORDER BY event_time DESC
                LIMIT %s
            )
            SELECT device_id, metric, value, unit, event_time
            FROM measurements
            WHERE device_id = %s
              AND event_time IN (SELECT event_time FROM selected_times)
            ORDER BY event_time DESC, metric ASC;
        """
        query_params.extend([params.limit, device_id])
        cur.execute(query, query_params)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return build_measurements_response(device_id, rows, metrics)
