"""Real PostgreSQL check for persisted PV dates; requires an isolated test DSN."""
import os
from pathlib import Path
import uuid

import pytest

psycopg2 = pytest.importorskip("psycopg2")
from psycopg2 import sql

ROOT = Path(__file__).resolve().parents[1]
DSN = os.getenv("TIME_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="Set TIME_TEST_DSN to an isolated PostgreSQL instance")
TARGETS = {
    "upat_devices": ("created_at",), "shelly_devices": ("created_at",),
    "upat_raw_messages": ("event_time", "ingestion_time"),
    "shelly_raw_messages": ("event_time", "ingestion_time"),
    "upat_measurements": ("event_time",), "shelly_measurements": ("event_time",),
    "upat_measurements_5min": ("bucket_start",),
    "upat_measurements_hourly": ("bucket_start",),
}


def run_script(conn, relative):
    source = (ROOT / relative).read_text()
    source = "\n".join(line for line in source.splitlines() if not line.startswith("\\"))
    with conn.cursor() as cur:
        cur.execute(source)


@pytest.fixture()
def database():
    admin = psycopg2.connect(DSN)
    admin.autocommit = True
    name = "time_migration_test_" + uuid.uuid4().hex
    with admin.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    params = psycopg2.extensions.parse_dsn(DSN)
    params["dbname"] = name
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")
            for table, columns in TARGETS.items():
                fields = ", ".join(f"{column} timestamptz" for column in columns)
                cur.execute(f"CREATE TABLE {table}(id int primary key, value double precision, {fields})")
            for table in ("pv_plant_readings_5m", "pv_device_readings_5m"):
                cur.execute(f"CREATE TABLE {table}(observed_at timestamptz NOT NULL, local_date date NOT NULL, value numeric)")
                cur.execute(f"INSERT INTO {table} VALUES ('2026-09-05T22:00:00Z', '2026-09-06', 4.2)")
        yield conn
    finally:
        conn.close()
        with admin.cursor() as cur:
            cur.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        admin.close()


def test_athens_calendar_is_enforced_when_pv_is_written(database):
    conn = database
    run_script(conn, "db/migrations/014_telemetry_timestamptz.sql")
    with conn.cursor() as cur:
        for table in ("pv_plant_readings_5m", "pv_device_readings_5m"):
            with pytest.raises(psycopg2.errors.CheckViolation):
                cur.execute(f"INSERT INTO {table} VALUES ('2026-09-05T22:00:00Z', '2026-09-05', 1)")
            cur.execute(f"SELECT observed_at,local_date,value FROM {table}")
            assert len(cur.fetchall()) == 1
