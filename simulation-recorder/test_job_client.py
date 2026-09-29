from unittest.mock import Mock

import pytest
import requests

import job_client as client
import main

RUN = "20260907_231004_e805bd97"
HANDLE = {"run_id": RUN, "status_url": f"/simulations/{RUN}"}
BODY = {"school_id": "school_3", "target_date": "2026-09-08"}
URL = "https://backend.example/simulate/day-ahead"


@pytest.mark.parametrize("status", [408, 425, 429, 500, 502, 503, 504])
def test_post_is_never_retried_on_http_error(monkeypatch, status):
    post = Mock(return_value=Mock(status_code=status))
    monkeypatch.setattr(main.requests, "post", post)
    assert main.fetch_simulation_response(URL, BODY, "token").status_code == status
    assert post.call_count == 1
    assert post.call_args.kwargs["allow_redirects"] is False


@pytest.mark.parametrize("path", ["https://evil.example/status", "//evil.example/status", "/simulations/wrong", "/simulations/../auth/login"])
def test_handle_cannot_redirect_token(monkeypatch, path):
    get = Mock()
    monkeypatch.setattr(client.requests, "get", get)
    with pytest.raises(client.SimulationPending):
        client.poll_simulation(URL, {**HANDLE, "status_url": path}, BODY, "token")
    get.assert_not_called()


def test_only_get_retries_and_terminal_failure_stops(monkeypatch):
    get = Mock(side_effect=[requests.ReadTimeout(), Mock(status_code=503),
                            Mock(status_code=200, json=lambda: {**HANDLE, "school_id": "school_3", "status": "failed"})])
    monkeypatch.setattr(client.requests, "get", get)
    monkeypatch.setattr(client.time, "sleep", lambda _: None)
    with pytest.raises(client.SimulationTerminalFailure):
        client.poll_simulation(URL, HANDLE, BODY, "token")
    assert get.call_count == 3
    assert get.call_args.kwargs["params"] == {"result_format": "day-ahead"}
    assert get.call_args.kwargs["allow_redirects"] is False


@pytest.mark.parametrize("status", [301, 302, 401, 403, 404, 410, 422])
def test_unavailable_status_preserves_admission(monkeypatch, status):
    get = Mock(return_value=Mock(status_code=status))
    monkeypatch.setattr(client.requests, "get", get)
    with pytest.raises(client.SimulationPending):
        client.poll_simulation(URL, HANDLE, BODY, "token")
    assert get.call_count == 1


def test_poll_budget_expires_without_submission(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(client.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(client.time, "sleep", lambda delay: clock.__setitem__(0, clock[0] + delay))
    monkeypatch.setattr(client, "POLL_TIMEOUT_SECONDS", 2)
    monkeypatch.setattr(client, "POLL_INTERVAL_SECONDS", 1)
    get = Mock(return_value=Mock(status_code=200, json=lambda: {**HANDLE, "school_id": "school_3", "status": "running"}))
    post = Mock()
    monkeypatch.setattr(client.requests, "get", get)
    monkeypatch.setattr(client.requests, "post", post)
    with pytest.raises(client.SimulationPending):
        client.poll_simulation(URL, HANDLE, BODY, "token")
    assert get.call_count == 2
    post.assert_not_called()


def valid_result():
    from datetime import datetime, timedelta
    start = datetime.fromisoformat("2026-09-08T00:00:00+03:00")
    return {
        "status": "completed_with_warnings", "simulation_engine": "energyplus",
        "run_id": RUN, "school_id": BODY["school_id"], "day_ahead_date": BODY["target_date"],
        "summary": {"requested_rooms": 1, "successful_rooms": 1, "failed_rooms": 0},
        "room_results": [{"room_id": "classroom", "status": "success",
                          "metrics": {"facility_kwh": 0.0, "cooling_kwh": None}}],
        "school_totals": {"facility_kwh": 0.0, "cooling_kwh": None},
        "hourly_load": {"complete": True, "unit": "kWh", "interval_minutes": 60,
                        "expected_intervals": 24, "items": [
            {"interval_start": (start + timedelta(hours=h)).isoformat(),
             "interval_end": (start + timedelta(hours=h+1)).isoformat(),
             "predicted_energy_kwh": 0.0} for h in range(24)]},
    }


def test_valid_zero_and_missing_optional_metrics_are_preserved():
    payload = valid_result()
    assert client.validate_day_ahead_result(payload, BODY) == payload["room_results"]
    assert payload["school_totals"] == {"facility_kwh": 0.0, "cooling_kwh": None}


@pytest.mark.parametrize("damage", [
    lambda p: p.update(status="partial_success"),
    lambda p: p.update(school_id="school_7"),
    lambda p: p.update(day_ahead_date="2026-09-09"),
    lambda p: p["summary"].update(failed_rooms=1),
    lambda p: p["room_results"][0].update(status="failed"),
    lambda p: p["hourly_load"].update(complete=False),
    lambda p: p["hourly_load"]["items"].pop(),
    lambda p: p["hourly_load"]["items"][0].update(predicted_energy_kwh=float("nan")),
    lambda p: p["hourly_load"]["items"][1].update(interval_start=p["hourly_load"]["items"][0]["interval_start"]),
    lambda p: p["school_totals"].update(facility_kwh=1),
    lambda p: p["room_results"][0]["metrics"].update(facility_kwh=float("inf")),
])
def test_incomplete_or_mismatched_forecast_cannot_be_successful(damage):
    payload = valid_result()
    damage(payload)
    with pytest.raises(client.SimulationTerminalFailure):
        client.validate_day_ahead_result(payload, BODY)
