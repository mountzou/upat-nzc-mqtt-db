"""Shelly storage integration tests against an explicit localhost fixture."""

import importlib.util
import json
import os
from pathlib import Path
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
import psycopg2
from psycopg2.extras import RealDictCursor
import pytest

ROOT = Path(__file__).resolve().parents[1]
DSN = os.getenv("SHELLY_COMPACT_TEST_DSN", "")
pytestmark = pytest.mark.skipif(not DSN, reason="Explicit isolated local DB required")
sys.path.insert(0, str(ROOT / "mqtt/shelly-devices"))
sys.path.insert(0, str(ROOT / "api"))


def load(name, path):
    s = importlib.util.spec_from_file_location(name, ROOT / path)
    m = importlib.util.module_from_spec(s)
    s.loader.exec_module(m)
    return m


writer = load("compact_writer", "mqtt/shelly-devices/measurements.py")
ingestor = load("compact_ingestor", "mqtt/shelly-devices/main.py")
START = datetime(2026, 9, 15, 12, tzinfo=timezone.utc)


def connection(**kwargs):
    assert "host=127.0.0.1" in DSN and "dbname=compact_fixture" in DSN
    return psycopg2.connect(DSN, **kwargs)


@pytest.fixture
def db():
    conn = connection()
    with conn, conn.cursor() as cur:
        cur.execute(
            "DROP SCHEMA IF EXISTS shelly_compact CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public"
        )
        cur.execute("""CREATE TABLE public.shelly_measurements(id SERIAL PRIMARY KEY,device_id text NOT NULL,metric text NOT NULL,value float8,unit text,event_time timestamptz);
   CREATE TABLE shelly_devices(source text,device_id text,name text,PRIMARY KEY(source,device_id));
   CREATE TABLE shelly_raw_messages(device_id text,topic text,payload jsonb,event_time timestamptz);
   CREATE TABLE shelly_energy_counters(device_id text,channel text,observed_at timestamptz,energy_wh float8,returned_energy_wh float8,counter_kind text,source_component text);""")
        cur.execute((ROOT / "db/migrations/017_shelly_compact_prepare.sql").read_text())
        cur.execute(
            "CREATE INDEX measurements_series_time_covering ON shelly_compact.measurements "
            "(series_id, event_time DESC) INCLUDE (value)"
        )
    yield conn
    conn.close()


def scalar(db, sql):
    with db.cursor() as c:
        c.execute(sql)
        return c.fetchone()[0]


def test_concurrent_first_series_creation_keeps_all_measurements(db):
    def write(worker):
        c = connection()
        try:
            for i in range(20):
                with c:
                    writer.insert_measurement(
                        c, "new", "metric", worker + i, None, START, mode="compact"
                    )
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(write, range(6)))
    assert scalar(db, "SELECT count(*) FROM shelly_compact.series") == 1
    assert scalar(db, "SELECT count(*) FROM shelly_compact.measurements") == 120


def test_autocommit_rejected(db):
    db.autocommit = True
    with pytest.raises(ValueError):
        writer.insert_measurement(db, "d", "p", 1, mode="compact")
    db.autocommit = False


def test_actual_mqtt_message_keeps_counters_and_raw_transaction(db, monkeypatch):
    monkeypatch.setattr(ingestor, "get_connection", lambda: db)
    monkeypatch.setattr(
        ingestor,
        "insert_measurement",
        lambda *a, **kw: writer.insert_measurement(*a, **kw, mode="compact"),
    )
    msg = SimpleNamespace(
        topic="shellyplugsg3-test/status/switch:0",
        payload=json.dumps(
            {
                "apower": 286.45,
                "aenergy": {"total": 100, "minute_ts": int(START.timestamp())},
                "ret_aenergy": {"total": 10},
            }
        ).encode(),
    )
    ingestor.on_message(None, None, msg)
    assert scalar(db, "SELECT count(*) FROM shelly_raw_messages") == 1
    assert scalar(db, "SELECT count(*) FROM shelly_energy_counters") == 1
    db.commit()
    assert scalar(db, "SELECT count(*) FROM shelly_compact.measurements") == 3
    assert scalar(db, "SELECT count(*) FROM public.shelly_measurements") == 0


def test_message_failure_rolls_back_device_raw_counters_and_measurements(
    db, monkeypatch
):
    monkeypatch.setattr(ingestor, "get_connection", lambda: db)
    monkeypatch.setattr(
        ingestor,
        "insert_measurement",
        lambda *a, **kw: writer.insert_measurement(*a, **kw, mode="compact"),
    )
    with db, db.cursor() as c:
        c.execute("ALTER TABLE shelly_compact.measurements ADD CHECK(value<0)")
    msg = SimpleNamespace(
        topic="shellyplugsg3-test/status/switch:0",
        payload=b'{"apower":1,"aenergy":{"total":100}}',
    )
    with pytest.raises(psycopg2.errors.CheckViolation):
        ingestor.on_message(None, None, msg)
    for t in [
        "shelly_devices",
        "shelly_raw_messages",
        "shelly_compact.measurements",
        "shelly_compact.series",
        "shelly_energy_counters",
    ]:
        assert scalar(db, "SELECT count(*) FROM " + t) == 0


def test_actual_api_history_and_latest_match_with_decimal_policy(db, monkeypatch):
    import main as api
    from readers import shelly_storage as storage
    from readers.measurements import fetch_device_latest
    from schemas import HistoryQueryParams

    for i, v in enumerate([286.1, 286.7, 286.6, 286.4, 0.04, 0.04, 0.14, -0.05]):
        with db:
            writer.insert_measurement(
                db,
                "shellypro3em-test",
                "a_act_power",
                v,
                "W",
                START + timedelta(minutes=i // 4, seconds=i),
                mode="compact",
            )
    factory = lambda: connection(cursor_factory=RealDictCursor)
    monkeypatch.setattr(api, "get_connection", factory)
    monkeypatch.setattr(storage, "ROUNDING", "decimal_1")
    monkeypatch.setattr(storage, "READ_STORAGE", "compact")
    params = HistoryQueryParams(
        start=START.isoformat(),
        end=(START + timedelta(hours=1)).isoformat(),
        interval="1m",
    )
    history = api.fetch_device_history(
        "shelly_measurements", "shellypro3em-test", params
    )
    latest = fetch_device_latest(
        "shelly_measurements",
        "shellypro3em-test",
        None,
        3,
        connection_factory=factory,
    )
    assert history == latest
    assert [item["measurements"]["a_act_power"]["value"] for item in history["items"]] == [0.0, 286.5]
    assert storage.source_and_average("upat_measurements") == (
        "upat_measurements",
        "AVG(value)",
    )
