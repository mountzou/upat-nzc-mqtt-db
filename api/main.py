import os
from fastapi import FastAPI, Query
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
def get_device_history(
    device_id: str,
    metric: str | None = Query(default=None),
    limit: int = Query(default=100, le=1000),
):
    conn = get_connection()
    cur = conn.cursor()

    if metric:
        cur.execute(
            """
            SELECT device_id, metric, value, unit, event_time
            FROM measurements
            WHERE device_id = %s AND metric = %s
            ORDER BY event_time DESC
            LIMIT %s;
            """,
            (device_id, metric, limit),
        )
    else:
        cur.execute(
            """
            SELECT device_id, metric, value, unit, event_time
            FROM measurements
            WHERE device_id = %s
            ORDER BY event_time DESC
            LIMIT %s;
            """,
            (device_id, limit),
        )

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows


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