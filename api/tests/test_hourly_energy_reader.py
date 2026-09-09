"""Hourly energy transport, coverage, DST and retained HTTP contracts."""
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock
from uuid import uuid4
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import pytest
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.testclient import TestClient

import database
import main
from monitoring import local_data, read_limits
from monitoring.services import service_energy_api
from monitoring.services.energy import school_insights
from readers.shelly_energy import fetch_shelly_hourly_energy_rows

UTC = timezone.utc
START = datetime(2026, 10, 25, tzinfo=UTC)
END = START + timedelta(hours=2)


def test_hourly_service_returns_native_rows_without_the_url_adapter(monkeypatch):
    payload = {"items": [{"window_start": START, "window_end": END}]}
    reader = Mock(return_value=payload)
    monkeypatch.setattr(service_energy_api, "read_hourly_energy", reader)
    monkeypatch.setattr(service_energy_api, "local_read",
                        Mock(side_effect=AssertionError("URL adapter used")))
    result = service_energy_api.fetch_shelly_hourly_energy(
        ["shellyplug-a", "shellypro3em-b"],
        start=START + timedelta(seconds=45, microseconds=123),
        end=END + timedelta(seconds=20), working_only=True,
    )
    assert result is payload
    assert result["items"][0]["window_start"] is START
    reader.assert_called_once_with(["shellyplug-a", "shellypro3em-b"], START, END, True)


@pytest.mark.parametrize("device_ids,end,exception,detail", [
    ([], END, ValueError, "fetch_shelly_hourly_energy requires at least one device_id"),
    (["shellyplug-a"], START - timedelta(minutes=1), HTTPException,
     "end must be greater than or equal to start"),
    (["shellyplug-a"], START + timedelta(days=90, minutes=1), HTTPException,
     "Energy history must not exceed 90 days"),
])
def test_invalid_service_windows_never_read(monkeypatch, device_ids, end, exception, detail):
    reader = Mock(side_effect=AssertionError("Invalid request reached reader"))
    monkeypatch.setattr(service_energy_api, "read_hourly_energy", reader)
    with pytest.raises(exception) as caught:
        service_energy_api.fetch_shelly_hourly_energy(device_ids, start=START, end=end)
    if exception is HTTPException:
        assert caught.value.status_code == 400
        assert caught.value.detail == detail
    else:
        assert str(caught.value) == detail
    reader.assert_not_called()


def test_busy_hourly_service_uses_the_shared_budget(monkeypatch):
    acquire = Mock(return_value=False)
    monkeypatch.setattr(read_limits._reads, "acquire", acquire)
    reader = Mock(side_effect=AssertionError("Busy request reached reader"))
    monkeypatch.setattr(service_energy_api, "read_hourly_energy", reader)
    with pytest.raises(HTTPException) as caught:
        service_energy_api.fetch_shelly_hourly_energy(["shellyplug-a"], start=START, end=END)
    assert caught.value.status_code == 503
    assert caught.value.detail == "Monitoring data is busy. Try again shortly."
    acquire.assert_called_once_with(timeout=5)
    reader.assert_not_called()


def test_hourly_reader_failure_releases_the_budget(monkeypatch):
    monkeypatch.setattr(service_energy_api, "read_hourly_energy",
                        Mock(side_effect=RuntimeError("fixture failure")))
    with pytest.raises(RuntimeError, match="fixture failure"):
        service_energy_api.fetch_shelly_hourly_energy(["shellyplug-a"], start=START, end=END)
    acquired = 0
    try:
        for _ in range(8):
            assert read_limits._reads.acquire(blocking=False)
            acquired += 1
        assert not read_limits._reads.acquire(blocking=False)
    finally:
        for _ in range(acquired):
            read_limits._reads.release()


def test_history_adapter_no_longer_dispatches_hourly_energy():
    with pytest.raises(ValueError, match="Unsupported local monitoring query"):
        local_data.local_read("/shelly/hourly-energy")


def test_insights_keep_repeated_hour_and_newest_duplicate_with_native_timestamps():
    athens = ZoneInfo("Europe/Athens")
    def row(stamp, value, age=0):
        return {"device_id": "shellyplug-a", "window_start": stamp.astimezone(athens),
                "window_end": (stamp + timedelta(hours=1)).astimezone(athens),
                "created_at": stamp + timedelta(hours=2, seconds=age),
                "energy_wh": {"total": value}}
    payload = {"items": [row(START, 0), row(START + timedelta(hours=1), 100),
                         row(START + timedelta(hours=1), 200, age=1)]}
    kwargs = {"device_ids": {"shellyplug-a"}, "start": START, "end": END}
    native = school_insights._normalize_history(payload, **kwargs)
    assert native == school_insights._normalize_history(jsonable_encoder(payload), **kwargs)
    assert native == ({"shellyplug-a": {START: 0.0, START + timedelta(hours=1): 200.0}},
                      END, 0, 1)
    assert school_insights._parse_datetime(START.replace(tzinfo=None)) is None


def test_complete_insights_equal_for_native_and_serialized_histories(monkeypatch):
    start = END - timedelta(days=90)
    items = []
    for i in range(90 * 24):
        if i % 101 == 0:  # Preserve missing data as well as measured zero.
            continue
        stamp = start + timedelta(hours=i)
        items.append({"device_id": "shellypro3em-a", "window_start": stamp,
                      "window_end": stamp + timedelta(hours=1), "created_at": stamp,
                      "energy_wh": {"total": 0.0 if i % 24 < 8 else 450.25}})
    devices = [{"id": "shellypro3em-a", "type": "three_phase_meter", "room_id": "all_rooms"}]
    results = []
    for payload in ({"items": items}, jsonable_encoder({"items": items})):
        monkeypatch.setattr(service_energy_api, "read_hourly_energy", Mock(return_value=payload))
        response = school_insights.build_school_energy_insights("fixture-school", devices=devices, now=END)
        results.append(response.model_dump(mode="json", exclude={"generated_at", "computation_ms"}))
    assert results[0] == results[1]
    assert results[0]["devices_analyzed"] == 1
    assert results[0]["data_through"] is not None
    assert results[0]["insights"]


@pytest.fixture
def hourly_database(monkeypatch):
    dsn = os.getenv("MONITORING_TEST_DSN")
    if not dsn:
        pytest.skip("Requires disposable local PostgreSQL")
    assert "host=127.0.0.1" in dsn and "local-fixture-only" in dsn
    schema = "hourly_reader_" + uuid4().hex
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}")
        for table, fields in [
            ("shelly_plug_hourly_energy", "energy_wh NUMERIC"),
            ("shelly_pro3em_hourly_energy", "a_energy_wh NUMERIC, b_energy_wh NUMERIC, "
             "c_energy_wh NUMERIC, total_energy_wh NUMERIC"),
        ]:
            cur.execute(f"""CREATE TABLE {schema}.{table} (
                device_id TEXT, window_start TIMESTAMPTZ, window_end TIMESTAMPTZ,
                {fields}, is_working_day INTEGER, is_working_hour INTEGER, created_at TIMESTAMPTZ)""")

    def connect():
        return psycopg2.connect(dsn, cursor_factory=RealDictCursor,
            options=f"-c search_path={schema} -c timezone=Europe/Athens")

    monkeypatch.setattr(database, "get_connection", connect)
    monkeypatch.setattr(main, "get_connection", connect)
    try:
        conn = connect()
        try:
            with conn, conn.cursor() as cur:
                for family in ("shellyplug", "shellypro3em"):
                    values = []
                    for i in range(-1, 3):
                        stamp = START + timedelta(hours=i)
                        energy = (None,) if family == "shellyplug" else (1.23456, None, 0, 1.23456)
                        values.append((family + "-a", stamp, stamp + timedelta(hours=1),
                                       *energy, 1, i % 2, stamp + timedelta(hours=1, seconds=5)))
                    table = "shelly_plug_hourly_energy" if family == "shellyplug" else "shelly_pro3em_hourly_energy"
                    execute_values(cur, f"INSERT INTO {table} VALUES %s", values)
        finally:
            conn.close()
        yield connect
    finally:
        with admin.cursor() as cur:
            cur.execute(f"DROP SCHEMA {schema} CASCADE")
        admin.close()


@pytest.mark.parametrize("working_only,per_device", [(False, 2), (True, 1)])
def test_hourly_sql_preserves_bounds_order_phase_values_and_dst(hourly_database, working_only, per_device):
    result = service_energy_api.fetch_shelly_hourly_energy(
        [" shellypro3em-a ", "shellyplug-a", "shellyplug-a"],
        start=START, end=END, working_only=working_only,
    )
    assert result["device_ids"] == ["shellyplug-a", "shellypro3em-a"]
    assert result["count"] == 2 * per_device
    assert [r["device_type"] for r in result["items"]] == ["plug"] * per_device + ["pro3em"] * per_device
    for rows in (result["items"][:per_device], result["items"][per_device:]):
        assert [r["window_start"].isoformat() for r in rows] == [
            "2026-10-25T03:00:00+02:00", "2026-10-25T03:00:00+03:00"][:per_device]
    assert result["items"][0]["energy_wh"] == {"total": 0.0}
    assert result["items"][-1]["energy_wh"] == {"a": 1.235, "b": 0.0, "c": 0.0, "total": 1.235}
    for ids, start, end in [(["shellyplug-missing"], START, END), (["shellyplug-a"], START, START),
                            (["shellyplug-a"], START + timedelta(minutes=1), END - timedelta(minutes=1))]:
        assert fetch_shelly_hourly_energy_rows(ids, start, end, False)["items"] == []


def test_retained_http_route_and_service_auth_match_direct_reader(hourly_database, monkeypatch):
    token = "isolated-hourly-reader-service-" + "x" * 40
    monkeypatch.setenv("DATA_SERVICE_TOKEN", token)
    params = [("device_id", "shellyplug-a"), ("device_id", "shellypro3em-a"),
              ("start", START.isoformat()), ("end", END.isoformat())]
    expected = jsonable_encoder(service_energy_api.fetch_shelly_hourly_energy(
        ["shellyplug-a", "shellypro3em-a"], start=START, end=END))
    with TestClient(main.app) as client:
        for path in ("/shelly/hourly-energy", "/internal/data/shelly/hourly-energy"):
            result = client.get(path, params=params, headers={"Authorization": "Bearer " + token})
            assert result.status_code == 200, result.text
            assert result.json() == expected
        assert client.get("/internal/data/shelly/hourly-energy", params=params).status_code == 401
        connection = Mock(side_effect=AssertionError("Invalid request reached DB"))
        monkeypatch.setattr(main, "get_connection", connection)
        for params, detail in [
            ({"start": "invalid"}, "At least one device_id must be provided"),
            ({"device_id": "unknown", "start": START.isoformat(), "end": END.isoformat()},
             "Unknown Shelly device type for device_ids=['unknown']"),
            ({"device_id": "shellyplug-a", "start": END.isoformat(), "end": START.isoformat()},
             "start must be earlier than or equal to end"),
        ]:
            response = client.get("/shelly/hourly-energy", params=params)
            assert response.status_code == 400
            assert response.json()["detail"] == detail
        connection.assert_not_called()
