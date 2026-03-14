import os
from fastapi import FastAPI, HTTPException, Query
import psycopg2
from psycopg2.extras import RealDictCursor

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
def get_latest_measurements(device_id: str):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT DISTINCT ON (metric)
            device_id,
            metric,
            value,
            unit,
            event_time
        FROM measurements
        WHERE device_id = %s
        ORDER BY metric, event_time DESC;
        """,
        (device_id,),
    )
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows


@app.get("/devices/{device_id}/history")
@app.get("/device/{device_id}/history")
def get_device_history(
    device_id: str,
    metric: str | None = Query(default=None),
    metrics: str | None = Query(default=None),
    limit: int = Query(default=100, le=1000),
    aggregate: str | None = Query(default=None),
    bucket: str | None = Query(default=None),
    bucket_unit: str | None = Query(default=None),
    bucket_size: int | None = Query(default=None, ge=1, le=10080),
):
    conn = get_connection()
    cur = conn.cursor()
    requested_metrics = None

    if metrics:
        requested_metrics = {
            item.strip()
            for item in metrics.split(",")
            if item.strip()
        }

    if aggregate is not None or bucket is not None or bucket_unit is not None or bucket_size is not None:
        if aggregate != "avg":
            raise HTTPException(
                status_code=400,
                detail="aggregate must be 'avg' when provided",
            )
        resolved_bucket_unit = bucket_unit or bucket

        if resolved_bucket_unit not in {"minute", "hour"}:
            raise HTTPException(
                status_code=400,
                detail="bucket or bucket_unit must be 'minute' or 'hour' when aggregate is used",
            )
        if bucket is not None and bucket_unit is not None and bucket != bucket_unit:
            raise HTTPException(
                status_code=400,
                detail="bucket and bucket_unit must match when both are provided",
            )

        resolved_bucket_size = bucket_size or 1
        bucket_interval = f"{resolved_bucket_size} {resolved_bucket_unit}"

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
        params = [bucket_interval, device_id]

        if metric:
            query += " AND metric = %s"
            params.append(metric)
        elif requested_metrics:
            query += " AND metric = ANY(%s)"
            params.append(sorted(requested_metrics))

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
        params.append(limit)
        cur.execute(query, params)
    elif metric:
        cur.execute(
            """
            WITH selected_times AS (
                SELECT DISTINCT event_time
                FROM measurements
                WHERE device_id = %s AND metric = %s
                ORDER BY event_time DESC
                LIMIT %s
            )
            SELECT device_id, metric, value, unit, event_time
            FROM measurements
            WHERE device_id = %s
              AND event_time IN (SELECT event_time FROM selected_times)
            ORDER BY event_time DESC, metric ASC;
            """,
            (device_id, metric, limit, device_id),
        )
    else:
        cur.execute(
            """
            WITH selected_times AS (
                SELECT DISTINCT event_time
                FROM measurements
                WHERE device_id = %s
                ORDER BY event_time DESC
                LIMIT %s
            )
            SELECT device_id, metric, value, unit, event_time
            FROM measurements
            WHERE device_id = %s
              AND event_time IN (SELECT event_time FROM selected_times)
            ORDER BY event_time DESC, metric ASC;
            """,
            (device_id, limit, device_id),
        )

    rows = cur.fetchall()

    cur.close()
    conn.close()

    snapshots = []
    snapshots_by_time = {}

    for row in rows:
        event_time = row["event_time"]
        event_time_key = event_time.isoformat() if event_time else "null"

        if requested_metrics and row["metric"] not in requested_metrics:
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
            "value": row["value"],
            "unit": row["unit"],
        }

    return {
        "device_id": device_id,
        "count": len(snapshots),
        "items": snapshots,
    }


@app.get("/measurements/recent")
def get_recent_measurements(limit: int = Query(default=20, le=200)):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT device_id, metric, value, unit, event_time
        FROM measurements
        ORDER BY event_time DESC
        LIMIT %s;
        """,
        (limit,),
    )
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows
