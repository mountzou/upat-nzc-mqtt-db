from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from fastapi import HTTPException

from monitoring.utils.timezone import as_utc, to_app_timezone
from monitoring.utils.api_datetime import normalize_api_window_dt
from monitoring.services.service_device_api import _parse_event_time
from monitoring.services.service_overview import get_device_latest_overview


@pytest.mark.parametrize("value", ["2026-09-06", "2026-09-06T12:00:00", "2026-10-25T03:30:00"])
def test_measurements_require_explicit_offsets(value):
    with pytest.raises(ValueError):
        _parse_event_time(value)
    with pytest.raises(ValueError):
        normalize_api_window_dt(datetime.fromisoformat(value))


def test_equivalent_offsets_and_athens_repeated_hour():
    assert _parse_event_time("2026-09-06T12:00:00+03:00") == _parse_event_time("2026-09-06T09:00:00Z")
    first = _parse_event_time("2026-10-25T03:30:00+03:00")
    second = _parse_event_time("2026-10-25T03:30:00+02:00")
    assert second - first == timedelta(hours=1)
    assert to_app_timezone(first).isoformat() == "2026-10-25T03:30:00+03:00"
    assert to_app_timezone(second).isoformat() == "2026-10-25T03:30:00+02:00"


def test_naive_upstream_payload_is_502(monkeypatch):
    monkeypatch.setattr("monitoring.services.service_overview.fetch_device_latest", lambda _: {
        "device_id": "fixture", "count": 1, "items": [{
            "device_id": "fixture", "event_time": "2026-09-06T12:00:00", "measurements": {}
        }]
    })
    with pytest.raises(HTTPException) as caught:
        get_device_latest_overview("fixture")
    assert caught.value.status_code == 502


def test_monitoring_query_rejects_missing_offset_before_service(monkeypatch):
    from fastapi.testclient import TestClient
    from main import app
    from monitoring.routes.auth import get_current_user
    from monitoring.services.authentication import AuthUserRecord
    monkeypatch.setitem(app.dependency_overrides, get_current_user,
        lambda: AuthUserRecord(username="test-admin", role="system_admin"))
    with TestClient(app) as client:
        for path, extra in [
            ("/energy/consumption/history", {"school_id": "school_10"}),
            ("/indoor_environment/devices/fixture/history", {}),
        ]:
            response = client.get(path, params={**extra, "start": "2026-09-06T00:00:00", "end": "2026-09-07T00:00:00Z"})
            assert response.status_code == 422
            assert any(error["type"] == "timezone_aware" for error in response.json()["detail"])


def test_history_order_uses_instants_across_repeated_athens_hour(monkeypatch):
    from monitoring.services.service_device_api import _fetch_batched_metric_history
    first = {"event_time": "2026-10-25T03:00:00+03:00"}
    second = {"event_time": "2026-10-25T03:00:00+02:00"}
    monkeypatch.setattr("monitoring.services.service_device_api._get_json", lambda *a, **kw: {"items": [first, second]})
    assert _fetch_batched_metric_history("fixture", "fixture", {}, ()) == [second, first]
