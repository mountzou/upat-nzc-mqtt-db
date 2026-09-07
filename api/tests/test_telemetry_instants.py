from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest

from schemas import HistoryQueryParams, parse_datetime_bound, parse_telemetry_bound
from monitoring.services import service_energy_api
from monitoring.utils.api_datetime import normalize_api_window_dt


@pytest.mark.parametrize("value", [
    "2026-09-06T12:00Z", "2026-09-06T15:00+03:00", "2026-09-06T08:00-04:00",
])
def test_explicit_offsets_refer_to_the_same_instant(value):
    expected = datetime(2026, 9, 6, 12, tzinfo=timezone.utc)
    assert parse_telemetry_bound(value, "start") == expected
    query = HistoryQueryParams(start=value, end="2026-09-06T13:00Z")
    assert query.resolved_start_time == expected


def test_fall_back_hours_remain_distinct():
    first = parse_telemetry_bound("2026-10-25T03:30+03:00", "start")
    second = parse_telemetry_bound("2026-10-25T03:30+02:00", "end")
    assert second - first == timedelta(hours=1)


def test_legacy_request_bounds_keep_their_utc_meaning_at_one_boundary():
    assert parse_telemetry_bound("2026-09-06T12:00", "start") == datetime(2026, 9, 6, 12, tzinfo=timezone.utc)
    assert parse_telemetry_bound("2026-09-06", "end") == datetime(2026, 9, 6, 23, 59, 59, 999999, tzinfo=timezone.utc)
    # Forecast wall-clock parsing has a separate contract.
    assert parse_datetime_bound("2026-09-06T12:00", "start").tzinfo is None


def test_internal_energy_requests_keep_the_instant_without_serializing(monkeypatch):
    original = datetime(2026, 9, 6, 15, tzinfo=timezone(timedelta(hours=3)))
    normalized = normalize_api_window_dt(original)
    reader = Mock(return_value={"items": []})
    monkeypatch.setattr(service_energy_api, "read_hourly_energy", reader)
    service_energy_api.fetch_shelly_hourly_energy(
        ["shellyplug-fixture"], start=original, end=original + timedelta(hours=1)
    )
    start, end = reader.call_args.args[1:3]
    assert normalized.tzinfo is not None
    assert start == original == normalized
    assert start.tzinfo is timezone.utc
    assert end - start == timedelta(hours=1)
