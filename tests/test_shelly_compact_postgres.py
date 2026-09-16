"""Real PostgreSQL migration tests, restricted to an explicit localhost fixture."""

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
sys.path.insert(0, str(ROOT / "shelly-ingestor"))
sys.path.insert(0, str(ROOT / "api"))


def load(name, path):
    s = importlib.util.spec_from_file_location(name, ROOT / path)
    m = importlib.util.module_from_spec(s)
    s.loader.exec_module(m)
    return m


writer = load("compact_writer", "shelly-ingestor/measurements.py")
ingestor = load("compact_ingestor", "shelly-ingestor/main.py")
migration = load("compact_migration", "ops/shelly-compact/migrate.py")
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
    migration.prepare(conn)
    migration.build_index(conn)
    yield conn
    conn.close()


def scalar(db, sql):
    with db.cursor() as c:
        c.execute(sql)
        return c.fetchone()[0]


def copy_all(db, direction="forward", size=7):
    migration.capture(db, direction)
    while migration.batch(db, direction, size) is not None:
        pass


def test_every_raw_field_preserved_including_nulls_nonfinite_and_signed_zero(db):
    for i, v in enumerate(
        [None, 0.0, -0.0, 0.04, 286.45, float("nan"), float("inf"), -float("inf")]
    ):
        with db:
            writer.insert_measurement(
                db,
                "dev/é",
                "power",
                v,
                None if i % 2 else "",
                None if i % 3 else START + timedelta(microseconds=i),
                mode="legacy",
            )
    copy_all(db)
    assert migration.verify(db)["results"]["forward"]["rows"] == 8


def test_dual_insert_atomic_and_dictionary_not_updated_per_row(db):
    for _ in range(4):
        with db:
            writer.insert_measurement(db, "same", "metric", 1, None, START, mode="dual")
    assert scalar(db, "SELECT count(*) FROM shelly_compact.series") == 1
    assert migration.verify(db)["results"]["forward"]["rows"] == 4


def test_failure_in_compact_insert_rolls_back_legacy_write(db):
    with db, db.cursor() as c:
        c.execute("ALTER TABLE shelly_compact.measurements ADD CHECK(value>=0)")
    with pytest.raises(psycopg2.errors.CheckViolation):
        with db:
            writer.insert_measurement(db, "d", "p", -1, "W", START, mode="dual")
    assert scalar(db, "SELECT count(*) FROM public.shelly_measurements") == 0
    assert scalar(db, "SELECT count(*) FROM shelly_compact.series") == 0


def test_concurrent_first_series_creation_and_monotonic_shared_ids(db):
    def write(worker):
        c = connection()
        try:
            for i in range(20):
                with c:
                    writer.insert_measurement(
                        c, "new", "metric", worker + i, None, START, mode="dual"
                    )
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(write, range(6)))
    assert scalar(db, "SELECT count(*) FROM shelly_compact.series") == 1
    db.commit()
    assert migration.verify(db)["results"]["forward"]["rows"] == 120


def test_resumable_copy_with_live_dual_writes(db):
    for i in range(30):
        with db:
            writer.insert_measurement(db, "old", "p", i, "W", START, mode="legacy")
    migration.capture(db, "forward")
    assert migration.batch(db, "forward", 7)["rows"] == 7

    def live():
        c = connection()
        try:
            for i in range(40):
                with c:
                    writer.insert_measurement(
                        c, "live", "p", i, "W", START, mode="dual"
                    )
        finally:
            c.close()

    with ThreadPoolExecutor() as pool:
        future = pool.submit(live)
        while migration.batch(db, "forward", 7) is not None:
            pass
        future.result()
    assert migration.verify(db)["results"]["forward"]["rows"] == 70


def test_out_of_order_legacy_transaction_must_drain_before_capture(db):
    c = connection()
    writer.insert_measurement(c, "uncommitted", "p", 1, None, START, mode="legacy")
    try:
        with pytest.raises(psycopg2.errors.LockNotAvailable):
            migration.capture(db, "forward")
        c.commit()
        copy_all(db)
        assert migration.verify(db)["results"]["forward"]["rows"] == 1
    finally:
        c.close()


def test_reverse_copy_preserves_compact_only_tail_for_rollback(db):
    with db:
        writer.insert_measurement(db, "d", "p", 1, "W", START, mode="dual")
    with db:
        writer.insert_measurement(db, "d", "p", 2, "W", START, mode="compact")
    with pytest.raises(ValueError):
        migration.verify(db)
    copy_all(db, "reverse")
    assert migration.verify(db)["results"]["forward"]["rows"] == 2
    with db:
        writer.insert_measurement(db, "d", "p", 3, "W", START, mode="legacy")
    assert scalar(db, "SELECT max(id) FROM public.shelly_measurements") == 3


def test_conflicting_existing_id_cannot_be_silently_skipped(db):
    with db:
        writer.insert_measurement(db, "d", "p", 1, "W", START, mode="dual")
    with db, db.cursor() as c:
        c.execute("UPDATE shelly_compact.measurements SET value=999")
    migration.capture(db, "forward")
    with pytest.raises(ValueError):
        migration.batch(db, "forward")
    assert (
        scalar(
            db,
            "SELECT last_id FROM shelly_compact.copy_progress WHERE direction='forward'",
        )
        == -2147483649
    )


def test_failed_batch_progress_resumes_without_loss(db):
    with db:
        writer.insert_measurement(db, "d", "p", 1, "W", START, mode="legacy")
    migration.capture(db, "forward")
    with db, db.cursor() as c:
        c.execute(
            "ALTER TABLE shelly_compact.measurements ADD CONSTRAINT reject_fixture CHECK(value<0)"
        )
    with pytest.raises(psycopg2.errors.CheckViolation):
        migration.batch(db, "forward")
    assert (
        scalar(
            db,
            "SELECT last_id FROM shelly_compact.copy_progress WHERE direction='forward'",
        )
        == -2147483649
    )
    with db, db.cursor() as c:
        c.execute(
            "ALTER TABLE shelly_compact.measurements DROP CONSTRAINT reject_fixture"
        )
    assert migration.batch(db, "forward")["inserted"] == 1
    assert migration.verify(db)["status"] == "PASS"


def test_autocommit_rejected(db):
    db.autocommit = True
    with pytest.raises(ValueError):
        writer.insert_measurement(db, "d", "p", 1, mode="dual")
    db.autocommit = False


def test_actual_mqtt_message_keeps_counters_and_raw_transaction(db, monkeypatch):
    monkeypatch.setattr(ingestor, "get_connection", lambda: db)
    monkeypatch.setattr(
        ingestor,
        "insert_measurement",
        lambda *a, **kw: writer.insert_measurement(*a, **kw, mode="dual"),
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
    assert migration.verify(db)["results"]["forward"]["rows"] == 3


def test_message_failure_rolls_back_device_raw_counters_and_measurements(
    db, monkeypatch
):
    monkeypatch.setattr(ingestor, "get_connection", lambda: db)
    monkeypatch.setattr(
        ingestor,
        "insert_measurement",
        lambda *a, **kw: writer.insert_measurement(*a, **kw, mode="dual"),
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
        "shelly_measurements",
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
                mode="dual",
            )
    factory = lambda: connection(cursor_factory=RealDictCursor)
    monkeypatch.setattr(api, "get_connection", factory)
    monkeypatch.setattr(storage, "ROUNDING", "decimal_1")
    results = []
    for mode in ["legacy", "compact"]:
        monkeypatch.setattr(storage, "READ_STORAGE", mode)
        params = HistoryQueryParams(
            start=START.isoformat(),
            end=(START + timedelta(hours=1)).isoformat(),
            interval="1m",
        )
        results.append(
            (
                api.fetch_device_history(
                    "shelly_measurements", "shellypro3em-test", params
                ),
                fetch_device_latest(
                    "shelly_measurements",
                    "shellypro3em-test",
                    None,
                    3,
                    connection_factory=factory,
                ),
            )
        )
    assert results[0] == results[1]
    assert results[0][0]["items"][-1]["measurements"]["a_act_power"]["value"] == 286.5
    assert storage.source_and_average("upat_measurements") == (
        "upat_measurements",
        "AVG(value)",
    )


def test_cli_checkpoint_and_copy_are_independent_transactions(
    db, monkeypatch, tmp_path
):
    monkeypatch.setenv("SHELLY_MIGRATION_DSN", DSN)
    with db:
        writer.insert_measurement(db, "cli", "p", 1, None, START, mode="legacy")
    for args in [
        ["capture", "--writers-paused"],
        ["copy", "--pause", "0", "--batch-size", "1"],
        ["index"],
        ["verify", "--receipt", str(tmp_path / "receipt.json")],
    ]:
        monkeypatch.setattr(sys, "argv", ["migrate.py", *args])
        migration.main()
    assert json.loads((tmp_path / "receipt.json").read_text())["status"] == "PASS"


def test_capacity_guard_stops_before_copy(db, monkeypatch, tmp_path):
    monkeypatch.setattr(
        migration.os, "statvfs", lambda _: SimpleNamespace(f_bavail=1, f_frsize=4096)
    )
    with pytest.raises(RuntimeError, match="free-space reserve"):
        migration.check_headroom(db, tmp_path)
    monkeypatch.setattr(
        migration.os,
        "statvfs",
        lambda _: SimpleNamespace(f_bavail=100 * 1024**3, f_frsize=1),
    )
    with pytest.raises(RuntimeError, match="retained WAL"):
        migration.check_headroom(db, tmp_path, maximum_wal_bytes=1)
    assert scalar(db, "SELECT count(*) FROM shelly_compact.measurements") == 0


def test_final_verification_requires_covering_index_and_rebuild_is_idempotent(db):
    with db, db.cursor() as cur:
        cur.execute("DROP INDEX shelly_compact.measurements_series_time_covering")
    with pytest.raises(ValueError, match="covering index"):
        migration.verify(db)
    assert migration.build_index(db)["already_valid"] is False
    assert migration.build_index(db)["already_valid"] is True
    assert migration.verify(db)["status"] == "PASS"
