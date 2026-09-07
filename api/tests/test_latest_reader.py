"""Latest-reader transport, concurrency and real PostgreSQL snapshot contracts."""
import os
from datetime import datetime, timezone
from unittest.mock import Mock
from uuid import uuid4

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

import database
import main
from monitoring import local_data, read_limits
from monitoring.routes import energy_demand, indoor_environment
from monitoring.routes.auth import get_current_user
from monitoring.services.authentication import AuthUserRecord
from monitoring.services.environment.history import service as environment_history
from monitoring.services import service_device_api, service_energy_api
from readers.measurements import fetch_device_latest


@pytest.mark.parametrize("family", ["upat", "shelly"])
def test_latest_services_use_direct_readers_with_native_timestamps(monkeypatch, family):
    module = service_device_api if family == "upat" else service_energy_api
    stamp = datetime(2026, 10, 25, 1, 30, tzinfo=timezone.utc)
    payload = {"device_id": "fixture", "count": 1,
               "items": [{"event_time": stamp}]}
    reader = Mock(return_value=payload)
    monkeypatch.setattr(module, "read_latest", reader)
    monkeypatch.setattr(module, "local_read", Mock(side_effect=AssertionError("URL adapter used")))
    if family == "upat":
        actual = module.fetch_device_latest("fixture")
        limit = 1
    else:
        actual = module.fetch_energy_device_latest("fixture", limit=4)
        limit = 4
    assert actual is payload
    assert actual["items"][0]["event_time"] is stamp
    reader.assert_called_once_with(f"{family}_measurements", "fixture", None, limit)


@pytest.mark.parametrize("family", ["upat", "shelly"])
def test_latest_busy_budget_rejects_before_reading(monkeypatch, family):
    assert environment_history._reads is read_limits._reads
    acquire = Mock(return_value=False)
    monkeypatch.setattr(read_limits._reads, "acquire", acquire)
    reader = Mock(side_effect=AssertionError("Busy request reached reader"))
    module = service_device_api if family == "upat" else service_energy_api
    monkeypatch.setattr(module, "read_latest", reader)
    with pytest.raises(HTTPException) as caught:
        if family == "upat":
            module.fetch_device_latest("fixture")
        else:
            module.fetch_energy_device_latest("fixture", limit=4)
    assert caught.value.status_code == 503
    assert caught.value.detail == "Monitoring data is busy. Try again shortly."
    acquire.assert_called_once_with(timeout=5)
    reader.assert_not_called()


def test_reader_failure_returns_the_shared_budget(monkeypatch):
    monkeypatch.setattr(service_device_api, "read_latest", Mock(side_effect=RuntimeError("fixture failure")))
    with pytest.raises(RuntimeError, match="fixture failure"):
        service_device_api.fetch_device_latest("fixture")
    acquired = 0
    try:
        for _ in range(8):
            assert read_limits._reads.acquire(blocking=False)
            acquired += 1
        assert not read_limits._reads.acquire(blocking=False)
    finally:
        for _ in range(acquired):
            read_limits._reads.release()


def test_history_adapter_no_longer_accepts_latest():
    with pytest.raises(ValueError, match="Unsupported local monitoring query"):
        local_data.local_read("/upat/device/fixture/latest")


@pytest.fixture
def latest_database(monkeypatch):
    dsn = os.getenv("MONITORING_TEST_DSN")
    if not dsn:
        pytest.skip("Requires disposable local PostgreSQL")
    assert "host=127.0.0.1" in dsn and "local-fixture-only" in dsn
    schema = "latest_fixture_" + uuid4().hex
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}")
        for family in ("upat", "shelly"):
            cur.execute(f"""CREATE TABLE {schema}.{family}_measurements (
                device_id TEXT, metric TEXT, value DOUBLE PRECISION,
                unit TEXT, event_time TIMESTAMPTZ)""")

    def connect():
        return psycopg2.connect(dsn, cursor_factory=RealDictCursor,
            options=f"-c search_path={schema} -c timezone=Europe/Athens")

    monkeypatch.setattr(database, "get_connection", connect)
    monkeypatch.setattr(main, "get_connection", connect)
    try:
        conn = connect()
        try:
            with conn, conn.cursor() as cur:
                for family in ("upat", "shelly"):
                    execute_values(cur, f"INSERT INTO {family}_measurements VALUES %s", [
                        ("fixture", "temperature", 20, "C", "2026-10-25T00:59:10Z"),
                        ("fixture", "temperature", 22, "C", "2026-10-25T00:59:40Z"),
                        ("fixture", "temperature", 24, "C", "2026-10-25T01:59:05Z"),
                        ("fixture", "temperature", 26, "C", "2026-10-25T01:59:15Z"),
                        ("fixture", "relative_humidity", 40, "%", "2026-10-25T01:59:20Z"),
                        ("fixture", "co2", 550, "ppm", "2026-10-25T02:00:00Z"),
                        ("other-device", "temperature", 99, "C", "2026-10-25T03:00:00Z"),
                    ])
        finally:
            conn.close()
        yield connect
    finally:
        with admin.cursor() as cur:
            cur.execute(f"DROP SCHEMA {schema} CASCADE")
        admin.close()


@pytest.mark.parametrize("family", ["upat", "shelly"])
def test_latest_sql_keeps_minute_means_metric_filter_and_dst_order(latest_database, family):
    result = fetch_device_latest(f"{family}_measurements", "fixture",
                                 [" temperature ", "temperature"], 2)
    assert result["device_id"] == "fixture"
    assert result["count"] == 2
    assert [i["event_time"].isoformat() for i in result["items"]] == [
        "2026-10-25T03:59:00+02:00", "2026-10-25T03:59:00+03:00"]
    assert [i["measurements"] for i in result["items"]] == [
        {"temperature": {"value": 25.0, "unit": "C"}},
        {"temperature": {"value": 21.0, "unit": "C"}}]
    newest = fetch_device_latest(f"{family}_measurements", "fixture", None, 1)
    assert newest["count"] == 1
    assert newest["items"][0]["measurements"] == {"co2": {"value": 550.0, "unit": "ppm"}}
    empty = fetch_device_latest(f"{family}_measurements", "missing-device", None, 30)
    assert empty == {"device_id": "missing-device", "count": 0, "items": []}


def test_latest_http_contracts_and_school_authorization(latest_database, monkeypatch):
    monkeypatch.setenv("DATA_SERVICE_TOKEN", "fixture-service-token-" + "x" * 40)
    user = AuthUserRecord(username="fixture", role="teacher", school_id="school_10",
                          school_ids=("school_10",))
    monkeypatch.setitem(main.app.dependency_overrides, get_current_user, lambda: user)
    monkeypatch.setattr(indoor_environment, "find_school_id_for_device", lambda _: "school_10")
    monkeypatch.setattr(energy_demand, "find_school_id_for_energy_device", lambda _: "school_10")
    with TestClient(main.app) as client:
        raw = client.get("/upat/device/fixture/latest", params={"metric": "temperature", "limit": 2})
        protected = client.get("/internal/data/upat/device/fixture/latest",
            params={"metric": "temperature", "limit": 2},
            headers={"Authorization": "Bearer " + os.environ["DATA_SERVICE_TOKEN"]})
        assert raw.status_code == protected.status_code == 200
        assert raw.json() == protected.json()
        assert raw.json()["items"][0]["event_time"] == "2026-10-25T03:59:00+02:00"
        overview = client.get("/indoor_environment/devices/fixture/latest")
        assert overview.status_code == 200
        assert overview.json()["latest_event_time"] == "2026-10-25T04:00:00+02:00"
        assert overview.json()["readings"] == {"co2": {"value": 550.0, "unit": "ppm"}}
        shelly = client.get("/energy/devices/fixture/latest", params={"limit": 2})
        assert shelly.status_code == 200 and shelly.json()["count"] == 2
        assert shelly.json() == client.get("/shelly/device/fixture/latest", params={"limit": 2}).json()

        connection = Mock(side_effect=AssertionError("Rejected request reached DB"))
        monkeypatch.setattr(database, "get_connection", connection)
        monkeypatch.setattr(main, "get_connection", connection)
        assert client.get("/internal/data/upat/device/fixture/latest").status_code == 401
        monkeypatch.setitem(main.app.dependency_overrides, get_current_user,
            lambda: AuthUserRecord(username="other", role="teacher", school_id="school_3",
                                  school_ids=("school_3",)))
        assert client.get("/indoor_environment/devices/fixture/latest").status_code == 403
        assert client.get("/energy/devices/fixture/latest").status_code == 403
        connection.assert_not_called()
