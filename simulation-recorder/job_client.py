"""Read-only polling and validation for the simulation recorder's admitted job."""

import math
import os
import re
import time
from datetime import datetime, time as calendar_time, timedelta, timezone
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import requests

POLL_INTERVAL_SECONDS = max(0.1, float(os.getenv("SIMULATION_POLL_INTERVAL_SECONDS", "5")))
POLL_TIMEOUT_SECONDS = max(1, float(os.getenv("SIMULATION_POLL_TIMEOUT_SECONDS", "1800")))
POLL_REQUEST_TIMEOUT_SECONDS = max(1, float(os.getenv("SIMULATION_POLL_REQUEST_TIMEOUT_SECONDS", "30")))
TRUSTED_STATUSES = {"success", "completed_with_warnings"}
RETRYABLE_GET_STATUSES = {408, 425, 429, 500, 502, 503, 504}
RUN_ID = re.compile(r"\d{8}_\d{6}_[0-9a-f]{8}")


class SimulationPending(RuntimeError):
    """Keep the persisted admission; another POST could duplicate the work."""


class SimulationTerminalFailure(ValueError):
    """The existing job failed. Do not resubmit it automatically."""


def canonical_handle(payload):
    if not isinstance(payload, dict):
        raise SimulationPending("Invalid simulation job handle; manual reconciliation required")
    run_id = payload.get("run_id")
    if not isinstance(run_id, str) or RUN_ID.fullmatch(run_id) is None:
        raise SimulationPending("Invalid simulation run id; manual reconciliation required")
    path = f"/simulations/{run_id}"
    if payload.get("status_url", path) != path:
        raise SimulationPending("Unexpected simulation status URL; manual reconciliation required")
    return {"run_id": run_id, "status_url": path}


def poll_simulation(request_url, handle, request_body, access_token):
    handle = canonical_handle(handle)
    origin = urlsplit(request_url)
    if origin.scheme != "https" or not origin.netloc or origin.username or origin.password:
        raise SimulationPending("Invalid configured simulation origin")
    status_url = f"{origin.scheme}://{origin.netloc}{handle['status_url']}"
    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise SimulationPending("Polling deadline reached; the saved job may still be running")
        response = None
        try:
            response = requests.get(
                status_url, params={"result_format": "day-ahead"},
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=(min(10, remaining), min(POLL_REQUEST_TIMEOUT_SECONDS, remaining)),
                allow_redirects=False,
            )
        except requests.RequestException:
            pass  # Only GET is retried. The saved admission remains authoritative.
        if response is not None and response.status_code not in RETRYABLE_GET_STATUSES:
            if response.status_code != 200:
                raise SimulationPending(f"Status HTTP {response.status_code}; saved job requires reconciliation")
            try:
                job = response.json()
            except ValueError:
                job = None
            if not isinstance(job, dict) or job.get("run_id") != handle["run_id"]:
                raise SimulationPending("Status response did not match the saved job")
            if job.get("school_id") != request_body["school_id"]:
                raise SimulationPending("Status response did not match the requested school")
            status = job.get("status")
            if status == "failed":
                raise SimulationTerminalFailure(f"Simulation job {handle['run_id']} failed")
            if status == "completed":
                result = job.get("result")
                validate_day_ahead_result(result, request_body, expected_run_id=handle["run_id"])
                return result
            if status not in {"queued", "running"}:
                raise SimulationPending("Unexpected state for the saved simulation job")
        time.sleep(min(POLL_INTERVAL_SECONDS, max(0, deadline - time.monotonic())))


def validate_day_ahead_result(payload, request_body, *, expected_run_id=None):
    """Validate identity, quality and complete hourly coverage before persistence."""
    if not isinstance(payload, dict) or payload.get("status") not in TRUSTED_STATUSES:
        raise SimulationTerminalFailure("Simulation did not return a trusted result")
    if (payload.get("school_id") != request_body["school_id"]
            or payload.get("day_ahead_date") != request_body["target_date"]
            or payload.get("simulation_engine") != "energyplus"):
        raise SimulationTerminalFailure("Simulation result identity/date did not match the request")
    canonical_handle({"run_id": payload.get("run_id")})
    if expected_run_id is not None and payload["run_id"] != expected_run_id:
        raise SimulationTerminalFailure("Completed result did not match the saved run id")
    rooms = payload.get("room_results")
    summary = payload.get("summary") or {}
    if not isinstance(summary, dict):
        raise SimulationTerminalFailure("Invalid simulation summary")
    if (not isinstance(rooms, list) or not rooms
            or summary.get("requested_rooms") != len(rooms)
            or summary.get("successful_rooms") != len(rooms)
            or summary.get("failed_rooms") != 0):
        raise SimulationTerminalFailure("Simulation did not complete every requested room")
    ids = []
    for room in rooms:
        if not isinstance(room, dict) or room.get("status") != "success" or not room.get("room_id"):
            raise SimulationTerminalFailure("Invalid or failed simulation room")
        if not isinstance(room["room_id"], str):
            raise SimulationTerminalFailure("Invalid room identifier")
        metrics = room.get("metrics")
        if not isinstance(metrics, dict):
            raise SimulationTerminalFailure("Invalid room metrics")
        for key, value in metrics.items():
            if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float))
                                      or not math.isfinite(value)
                                      or (key != "average_air_temperature_c" and value < 0)):
                raise SimulationTerminalFailure("Invalid room metric value")
        ids.append(room["room_id"])
    if len(set(ids)) != len(ids):
        raise SimulationTerminalFailure("Simulation returned duplicate rooms")
    hourly = payload.get("hourly_load") or {}
    if not isinstance(hourly, dict):
        raise SimulationTerminalFailure("Invalid hourly load")
    zone = ZoneInfo("Europe/Athens")
    day = datetime.fromisoformat(request_body["target_date"]).date()
    cursor = datetime.combine(day, calendar_time.min, tzinfo=zone).astimezone(timezone.utc)
    stop = datetime.combine(day + timedelta(days=1), calendar_time.min, tzinfo=zone).astimezone(timezone.utc)
    expected_intervals = int((stop - cursor) / timedelta(hours=1))
    points = hourly.get("items")
    if (hourly.get("complete") is not True or hourly.get("unit") != "kWh"
            or hourly.get("interval_minutes") != 60
            or hourly.get("expected_intervals") != expected_intervals
            or not isinstance(points, list) or len(points) != expected_intervals):
        raise SimulationTerminalFailure("Simulation hourly load is incomplete")
    total = 0.0
    for point in points:
        try:
            start = datetime.fromisoformat(point["interval_start"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(point["interval_end"].replace("Z", "+00:00"))
            value = point["predicted_energy_kwh"]
            if (start.utcoffset() is None or end.utcoffset() is None or start != cursor
                    or end.astimezone(timezone.utc) - start.astimezone(timezone.utc) != timedelta(hours=1)
                    or isinstance(value, bool) or not isinstance(value, (int, float))
                    or not math.isfinite(value) or value < 0):
                raise ValueError()
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise SimulationTerminalFailure("Invalid hourly load bounds or energy") from exc
        cursor = end.astimezone(timezone.utc)
        total += value
    totals = payload.get("school_totals")
    if not isinstance(totals, dict):
        raise SimulationTerminalFailure("Invalid school totals")
    for value in totals.values():
        if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float))
                                  or not math.isfinite(value) or value < 0):
            raise SimulationTerminalFailure("Invalid school total value")
    facility = totals.get("facility_kwh")
    if (cursor != stop or isinstance(facility, bool) or not isinstance(facility, (float, int))
            or not math.isfinite(facility) or not math.isclose(total, facility, rel_tol=1e-6, abs_tol=1e-5)):
        raise SimulationTerminalFailure("Hourly load does not match the school day/total")
    return rooms
