"""Real PostgreSQL checks for migration 014; requires an isolated test DSN."""
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
                fields = ", ".join(f"{column} timestamp" for column in columns)
                cur.execute(f"CREATE TABLE {table}(id int primary key, value double precision, {fields})")
                for column in columns:
                    cur.execute(f"CREATE INDEX {table}_{column}_test_idx ON {table}({column})")
                stamps = ["2026-01-01 00:00:00", "2026-03-29 00:30:00", "2026-03-29 01:30:00",
                          "2026-10-25 00:30:00", "2026-10-25 01:30:00", None]
                for index, stamp in enumerate(stamps):
                    cur.execute(f"INSERT INTO {table} VALUES ({', '.join(['%s'] * (2 + len(columns)))})",
                                (index, index + 0.25, *([stamp] * len(columns))))
            for table in ("pv_plant_readings_5m", "pv_device_readings_5m"):
                cur.execute(f"CREATE TABLE {table}(observed_at timestamptz NOT NULL, local_date date NOT NULL, value numeric)")
                cur.execute(f"INSERT INTO {table} VALUES ('2026-09-05T22:00:00Z', '2026-09-06', 4.2)")
        yield conn, params
    finally:
        conn.close()
        with admin.cursor() as cur:
            cur.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        admin.close()


def snapshot(conn, aware):
    result = {}
    with conn.cursor() as cur:
        for table, columns in TARGETS.items():
            fields = ", ".join(f"extract(epoch FROM {c})" if aware else
                               f"extract(epoch FROM {c} AT TIME ZONE 'UTC')" for c in columns)
            cur.execute(f"SELECT id, value, {fields} FROM {table} ORDER BY id")
            result[table] = cur.fetchall()
    return result


def test_migration_preserves_instants_values_nulls_heap_and_rollback(database):
    conn, params = database
    before = snapshot(conn, False)
    with conn.cursor() as cur:
        cur.execute("SELECT relname,relfilenode FROM pg_class WHERE relname = ANY(%s)", (list(TARGETS),))
        files = dict(cur.fetchall())
        # Deliberately start in Athens: the migration must set its own UTC rule.
        cur.execute("SET TIME ZONE 'Europe/Athens'")
    run_script(conn, "db/migrations/014_telemetry_timestamptz.sql")
    assert snapshot(conn, True) == before
    with conn.cursor() as cur:
        cur.execute("SELECT relname,relfilenode FROM pg_class WHERE relname = ANY(%s)", (list(TARGETS),))
        assert dict(cur.fetchall()) == files
        cur.execute("SELECT count(*) FROM pg_index WHERE NOT indisvalid OR NOT indisready")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT event_time::text FROM upat_measurements WHERE id IN (3,4) ORDER BY id")
        assert cur.fetchall() == [("2026-10-25 03:30:00+03",), ("2026-10-25 03:30:00+02",)]
    fresh = psycopg2.connect(**params)
    try:
        with fresh.cursor() as cur:
            cur.execute("SHOW timezone")
            assert cur.fetchone()[0] == "Europe/Athens"
    finally:
        fresh.close()
    run_script(conn, "db/migrations/014_telemetry_timestamptz.sql")
    assert snapshot(conn, True) == before
    run_script(conn, "db/maintenance/rollback_014_telemetry_timestamptz.sql")
    assert snapshot(conn, False) == before


def test_athens_calendar_is_enforced_when_pv_is_written(database):
    conn, _ = database
    run_script(conn, "db/migrations/014_telemetry_timestamptz.sql")
    with conn.cursor() as cur:
        for table in ("pv_plant_readings_5m", "pv_device_readings_5m"):
            with pytest.raises(psycopg2.errors.CheckViolation):
                cur.execute(f"INSERT INTO {table} VALUES ('2026-09-05T22:00:00Z', '2026-09-05', 1)")
            cur.execute(f"SELECT observed_at,local_date,value FROM {table}")
            assert len(cur.fetchall()) == 1


def test_invalid_pv_calendar_rolls_back_the_entire_migration(database):
    conn, _ = database
    before = snapshot(conn, False)
    with conn.cursor() as cur:
        cur.execute("UPDATE pv_plant_readings_5m SET local_date='2026-09-05'")
    with pytest.raises(psycopg2.errors.CheckViolation):
        run_script(conn, "db/migrations/014_telemetry_timestamptz.sql")
    with conn.cursor() as cur:
        cur.execute("ROLLBACK")
    assert snapshot(conn, False) == before
