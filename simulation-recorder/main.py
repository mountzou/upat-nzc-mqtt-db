import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import psycopg2
import requests
from psycopg2.extras import Json

from job_client import (
    SimulationPending, SimulationTerminalFailure, canonical_handle,
    poll_simulation, validate_day_ahead_result,
)

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_INTERNAL_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_CONNECT_RETRIES = int(os.getenv("POSTGRES_CONNECT_RETRIES", "5"))
DB_CONNECT_DELAY_SECONDS = float(os.getenv("POSTGRES_CONNECT_DELAY_SECONDS", "2"))

SIMULATION_API_BASE_URL = "https://upat-nzc-energyplus-backend.onrender.com"
SIMULATION_API_AUTH_PATH = "/auth/login"
SIMULATION_API_PATH = "/simulate/day-ahead"
SIMULATION_API_USERNAME = os.getenv("SIMULATION_API_USERNAME", "").strip()
SIMULATION_API_PASSWORD = os.getenv("SIMULATION_API_PASSWORD", "")
DEFAULT_SIMULATION_SCHOOL_IDS = [
    "school_3",
    "school_7",
    "school_10",
    "school_13",
    "school_22",
    "school_23",
]
SIMULATION_AUTH_TIMEOUT_SECONDS = float(
    os.getenv("SIMULATION_AUTH_TIMEOUT_SECONDS", "30")
)
SIMULATION_CONNECT_TIMEOUT_SECONDS = float(
    os.getenv("SIMULATION_CONNECT_TIMEOUT_SECONDS", "10")
)
SIMULATION_REQUEST_TIMEOUT_SECONDS = float(
    os.getenv("SIMULATION_REQUEST_TIMEOUT_SECONDS", "600")
)
SIMULATION_BETWEEN_SCHOOLS_DELAY_SECONDS = float(
    os.getenv("SIMULATION_BETWEEN_SCHOOLS_DELAY_SECONDS", "15")
)
SIMULATION_RECORDING_TIMEZONE = "Europe/Athens"
LOCAL_TZ = ZoneInfo(SIMULATION_RECORDING_TIMEZONE)


class SimulationAuthenticationError(RuntimeError):
    """Raised when the recorder cannot obtain a simulation API bearer token."""


def parse_simulation_school_ids(raw_value):
    if raw_value is None:
        return list(DEFAULT_SIMULATION_SCHOOL_IDS)

    values = (value.strip() for value in raw_value.split(","))
    school_ids = list(dict.fromkeys(filter(None, values)))

    if not school_ids:
        raise ValueError(
            "SIMULATION_SCHOOL_IDS must contain at least one school id"
        )

    unsupported = sorted(set(school_ids) - set(DEFAULT_SIMULATION_SCHOOL_IDS))
    if unsupported:
        raise ValueError(
            "SIMULATION_SCHOOL_IDS contains unsupported school ids: "
            f"{', '.join(unsupported)}"
        )

    return school_ids


SIMULATION_SCHOOL_IDS = parse_simulation_school_ids(
    os.getenv("SIMULATION_SCHOOL_IDS")
)


def db_connect():
    connection_error = None

    for attempt in range(1, DB_CONNECT_RETRIES + 1):
        try:
            return psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
            )
        except psycopg2.OperationalError as exc:
            connection_error = exc
            print(
                "Postgres connection failed "
                f"(attempt {attempt}/{DB_CONNECT_RETRIES}): {exc}"
            )
            if attempt < DB_CONNECT_RETRIES:
                time.sleep(DB_CONNECT_DELAY_SECONDS)

    raise connection_error


def build_simulation_api_url(path):
    base_url = SIMULATION_API_BASE_URL.rstrip("/") + "/"
    return urljoin(base_url, path.lstrip("/"))


def build_simulation_url():
    return build_simulation_api_url(SIMULATION_API_PATH)


def build_simulation_auth_url():
    return build_simulation_api_url(SIMULATION_API_AUTH_PATH)


def fetch_simulation_access_token(auth_url, username, password):
    if not username:
        raise SimulationAuthenticationError(
            "SIMULATION_API_USERNAME is required"
        )
    if not password:
        raise SimulationAuthenticationError(
            "SIMULATION_API_PASSWORD is required"
        )

    try:
        response = requests.post(
            auth_url,
            json={"username": username, "password": password},
            timeout=SIMULATION_AUTH_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        raise SimulationAuthenticationError(
            "Simulation API login request failed"
        ) from exc

    if not response.ok:
        raise SimulationAuthenticationError(
            f"Simulation API login returned HTTP {response.status_code}"
        )

    try:
        payload = response.json()
    except ValueError as exc:
        raise SimulationAuthenticationError(
            "Simulation API login response is not valid JSON"
        ) from exc

    access_token = payload.get("access_token") if isinstance(payload, dict) else None
    token_type = payload.get("token_type") if isinstance(payload, dict) else None
    if not isinstance(access_token, str) or not access_token.strip():
        raise SimulationAuthenticationError(
            "Simulation API login response is missing access_token"
        )
    if not isinstance(token_type, str) or token_type.lower() != "bearer":
        raise SimulationAuthenticationError(
            "Simulation API login response has an unsupported token_type"
        )
    return access_token.strip()


def utc_now():
    return datetime.now(timezone.utc)


def decimal_or_none(value):
    if value is None:
        return None

    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def int_or_none(value):
    if value is None:
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def date_or_none(value):
    if not value:
        return None

    if isinstance(value, date):
        return value

    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def create_day_ahead_run(conn, school_id, recording_date, request_url, request_path, request_body, started_at):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO simulation_day_ahead_runs (
                school_id,
                recording_date,
                request_url,
                request_path,
                request_body,
                started_at,
                success
            )
            VALUES (%s, %s, %s, %s, %s, %s, FALSE)
            RETURNING id;
            """,
            (
                school_id,
                recording_date,
                request_url,
                request_path,
                Json(request_body),
                started_at,
            ),
        )
        return cur.fetchone()[0]


def find_successful_day_ahead_run_id(conn, school_id, target_date):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM simulation_day_ahead_runs
            WHERE school_id = %s
              AND day_ahead_date = %s
              AND success IS TRUE
              AND status IN ('success', 'completed_with_warnings')
              AND requested_rooms > 0 AND successful_rooms = requested_rooms
              AND failed_rooms = 0
              AND response_json->'hourly_load'->>'complete' = 'true'
            ORDER BY completed_at DESC NULLS LAST, id DESC
            LIMIT 1;
            """,
            (school_id, target_date),
        )
        row = cur.fetchone()
        return row[0] if row else None


def finish_failed_day_ahead_run(conn, run_id, http_status, response_json, error_text):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE simulation_day_ahead_runs
            SET
                completed_at = %s,
                http_status = %s,
                success = FALSE,
                status = 'failed',
                response_json = %s,
                error_text = %s,
                updated_at = NOW()
            WHERE id = %s;
            """,
            (
                utc_now(),
                http_status,
                Json(response_json) if response_json is not None else None,
                error_text,
                run_id,
            ),
        )


def finish_successful_day_ahead_run(conn, run_id, http_status, response_json):
    summary = response_json.get("summary") if isinstance(response_json.get("summary"), dict) else {}
    totals = response_json.get("school_totals") if isinstance(response_json.get("school_totals"), dict) else {}

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE simulation_day_ahead_runs
            SET
                completed_at = %s,
                http_status = %s,
                status = %s,
                simulation_engine = %s,
                external_run_id = %s,
                day_ahead_date = %s,
                requested_rooms = %s,
                successful_rooms = %s,
                failed_rooms = %s,
                facility_kwh = %s,
                equipment_kwh = %s,
                lighting_kwh = %s,
                heating_liters = %s,
                cooling_kwh = %s,
                fans_hvac_kwh = %s,
                success = TRUE,
                error_text = NULL,
                response_json = %s,
                updated_at = NOW()
            WHERE id = %s;
            """,
            (
                utc_now(),
                http_status,
                response_json.get("status"),
                response_json.get("simulation_engine"),
                response_json.get("run_id"),
                date_or_none(response_json.get("day_ahead_date")),
                int_or_none(summary.get("requested_rooms")),
                int_or_none(summary.get("successful_rooms")),
                int_or_none(summary.get("failed_rooms")),
                decimal_or_none(totals.get("facility_kwh")),
                decimal_or_none(totals.get("equipment_kwh")),
                decimal_or_none(totals.get("lighting_kwh")),
                decimal_or_none(totals.get("heating_liters")),
                decimal_or_none(totals.get("cooling_kwh")),
                decimal_or_none(totals.get("fans_hvac_kwh")),
                Json(response_json),
                run_id,
            ),
        )


def build_simulation_request_body(school_id, target_date=None):
    resolved_target_date = target_date or (
        datetime.now(LOCAL_TZ).date() + timedelta(days=1)
    )
    return {
        "school_id": school_id,
        "target_date": resolved_target_date.isoformat(),
    }


def fetch_simulation_response(request_url, request_body, access_token):
    # A POST can start work even if its response is lost or is an HTTP 5xx.
    # Only status GETs may be retried; a new POST is never a retry strategy.
    try:
        return requests.post(
            request_url, json=request_body,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=(SIMULATION_CONNECT_TIMEOUT_SECONDS, SIMULATION_REQUEST_TIMEOUT_SECONDS),
            allow_redirects=False,
        )
    except requests.RequestException:
        print("Simulation request not retried because server-side completion is unknown")
        raise


def extract_room_results(response_json):
    if not isinstance(response_json, dict):
        raise ValueError("Day-ahead simulation response must be a JSON object")

    room_results = response_json.get("room_results")
    if not isinstance(room_results, list):
        raise ValueError("Day-ahead simulation response must contain room_results as a list")

    return room_results


def insert_day_ahead_room_results(conn, run_id, school_id, recording_date, room_results):
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM simulation_day_ahead_room_results
            WHERE run_id = %s;
            """,
            (run_id,),
        )

        inserted_count = 0
        for result in room_results:
            if not isinstance(result, dict):
                raise ValueError("Day-ahead room result items must be objects")

            room_id = result.get("room_id")
            if not room_id:
                raise ValueError("Day-ahead room result is missing room_id")

            metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}

            cur.execute(
                """
                INSERT INTO simulation_day_ahead_room_results (
                    run_id,
                    school_id,
                    recording_date,
                    room_id,
                    room_label,
                    status,
                    error_text,
                    average_air_temperature_c,
                    thermal_discomfort_hours,
                    facility_kwh,
                    equipment_kwh,
                    lighting_kwh,
                    heating_liters,
                    cooling_kwh,
                    fans_hvac_kwh,
                    raw_result
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (run_id, room_id)
                DO UPDATE SET
                    school_id = EXCLUDED.school_id,
                    recording_date = EXCLUDED.recording_date,
                    room_label = EXCLUDED.room_label,
                    status = EXCLUDED.status,
                    error_text = EXCLUDED.error_text,
                    average_air_temperature_c = EXCLUDED.average_air_temperature_c,
                    thermal_discomfort_hours = EXCLUDED.thermal_discomfort_hours,
                    facility_kwh = EXCLUDED.facility_kwh,
                    equipment_kwh = EXCLUDED.equipment_kwh,
                    lighting_kwh = EXCLUDED.lighting_kwh,
                    heating_liters = EXCLUDED.heating_liters,
                    cooling_kwh = EXCLUDED.cooling_kwh,
                    fans_hvac_kwh = EXCLUDED.fans_hvac_kwh,
                    raw_result = EXCLUDED.raw_result,
                    updated_at = NOW();
                """,
                (
                    run_id,
                    school_id,
                    recording_date,
                    room_id,
                    result.get("room_label"),
                    result.get("status"),
                    result.get("error"),
                    decimal_or_none(metrics.get("average_air_temperature_c")),
                    decimal_or_none(metrics.get("thermal_discomfort_hours")),
                    decimal_or_none(metrics.get("facility_kwh")),
                    decimal_or_none(metrics.get("equipment_kwh")),
                    decimal_or_none(metrics.get("lighting_kwh")),
                    decimal_or_none(metrics.get("heating_liters")),
                    decimal_or_none(metrics.get("cooling_kwh")),
                    decimal_or_none(metrics.get("fans_hvac_kwh")),
                    Json(result),
                ),
            )
            inserted_count += 1

    return inserted_count


def find_day_ahead_attempt(conn, school_id, target_date):
    """An unresolved previous admission must never be replaced automatically."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, external_run_id, status, response_json
            FROM simulation_day_ahead_runs
            WHERE school_id = %s
              AND (day_ahead_date = %s OR request_body->>'target_date' = %s)
            ORDER BY id DESC LIMIT 1;
            """, (school_id, target_date, target_date.isoformat()),
        )
        return cur.fetchone()


def remember_admission(conn, run_id, target_date, *, handle=None, message=None,
                       response_json=None, http_status=None):
    """Commit the handle before polling, independently of eventual result writes."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE simulation_day_ahead_runs SET
                external_run_id = COALESCE(%s, external_run_id),
                day_ahead_date = %s, status = %s, success = FALSE,
                error_text = %s, response_json = COALESCE(%s, response_json),
                http_status = COALESCE(%s, http_status),
                completed_at = NULL, updated_at = NOW()
            WHERE id = %s;
            """,
            (handle['run_id'] if handle else None, target_date,
             'pending' if handle else 'submission_unknown', message,
             Json(response_json) if response_json is not None else None, http_status, run_id),
        )
    conn.commit()


def run_school(conn, request_url, school_id, access_token):
    started_at = utc_now()
    recording_date = started_at.astimezone(LOCAL_TZ).date()
    target_date = recording_date + timedelta(days=1)
    request_body = build_simulation_request_body(school_id, target_date)
    existing_success = find_successful_day_ahead_run_id(conn, school_id, target_date)
    if existing_success is not None:
        print(f"Skipping day-ahead simulation: school_id={school_id}, existing_successful_run_id={existing_success}")
        return None

    previous = find_day_ahead_attempt(conn, school_id, target_date)
    handle = None
    response_json = None
    http_status = None
    if previous is not None:
        run_id, external_id, previous_status, response_json = previous
        if previous_status == 'failed' or external_id is None:
            return SimulationPending(
                f"Existing recording {run_id} requires reconciliation; no new POST was sent"
            )
        # A completed but rejected legacy result also needs explicit reconciliation.
        if previous_status not in ('pending', 'submission_unknown'):
            return SimulationPending(f"Recording {run_id} is not a trusted completed forecast")
        try:
            handle = canonical_handle({'run_id': external_id})
        except SimulationPending as exc:
            return exc
    else:
        run_id = create_day_ahead_run(
            conn, school_id, recording_date, request_url, SIMULATION_API_PATH,
            request_body, started_at,
        )
        # A crash after this commit is an unknown submission, never permission to repost.
        remember_admission(conn, run_id, target_date)

    result_ready = False
    conn.commit()  # Close the lookup transaction before waiting on HTTP.
    try:
        if handle is None:
            response = fetch_simulation_response(request_url, request_body, access_token)
            http_status = response.status_code
            try:
                response_json = response.json()
            except ValueError:
                response_json = None
            detail = response_json.get('detail') if isinstance(response_json, dict) else None
            if http_status == 504 and isinstance(detail, dict) and detail.get('code') == 'simulation_wait_timeout':
                handle = canonical_handle(detail)
                remember_admission(conn, run_id, target_date, handle=handle,
                                   response_json=response_json, http_status=http_status)
            elif http_status in (502, 503) and isinstance(detail, dict) and detail.get('code') in ('simulation_failed', 'simulation_worker_unavailable'):
                handle = canonical_handle(detail)
                remember_admission(conn, run_id, target_date, handle=handle,
                                   response_json=response_json, http_status=http_status)
                raise SimulationTerminalFailure(f"Simulation job {handle['run_id']} failed")
            elif http_status == 200:
                validate_day_ahead_result(response_json, request_body)
                handle = canonical_handle({'run_id': response_json['run_id']})
                remember_admission(conn, run_id, target_date, handle=handle,
                                   response_json=response_json, http_status=http_status)
                result_ready = True
            elif 400 <= http_status < 500 and http_status not in (408, 425, 429):
                raise SimulationTerminalFailure(f"Simulation request rejected: HTTP {http_status}")
            else:
                raise SimulationPending(f"Submission HTTP {http_status}; outcome requires reconciliation")
        if handle is not None and not result_ready:
            response_json = poll_simulation(request_url, handle, request_body, access_token)
            http_status = 200
        room_results = validate_day_ahead_result(response_json, request_body)
        finish_successful_day_ahead_run(conn, run_id, http_status, response_json)
        result_count = insert_day_ahead_room_results(
            conn, run_id, school_id, recording_date, room_results,
        )
        conn.commit()
        print(f"Day-ahead simulation completed: school_id={school_id}, run_id={run_id}, room_results={result_count}")
        return None
    except (SimulationPending, requests.RequestException) as exc:
        message = str(exc) if isinstance(exc, SimulationPending) else 'Transport failed; server-side completion is unknown'
        remember_admission(conn, run_id, target_date, handle=handle, message=message,
                           response_json=response_json, http_status=http_status)
        print(f"Day-ahead simulation pending: school_id={school_id}, run_id={run_id}, reason={message}")
        return SimulationPending(message)
    except ValueError as exc:
        # Roll back any incomplete result/room writes; the admission was already committed.
        conn.rollback()
        finish_failed_day_ahead_run(conn, run_id, http_status, response_json, str(exc))
        conn.commit()
        print(f"Day-ahead simulation failed: school_id={school_id}, run_id={run_id}, error={exc}")
        return exc


def run():
    request_url = build_simulation_url()
    auth_url = build_simulation_auth_url()
    failures = []

    print("Starting simulation recorder")
    print(f"SIMULATION_API_BASE_URL={SIMULATION_API_BASE_URL}")
    print(f"SIMULATION_API_PATH={SIMULATION_API_PATH}")
    print(f"SIMULATION_SCHOOL_IDS={','.join(SIMULATION_SCHOOL_IDS)}")
    print(
        "SIMULATION_BETWEEN_SCHOOLS_DELAY_SECONDS="
        f"{SIMULATION_BETWEEN_SCHOOLS_DELAY_SECONDS}"
    )

    try:
        access_token = fetch_simulation_access_token(
            auth_url,
            SIMULATION_API_USERNAME,
            SIMULATION_API_PASSWORD,
        )
    except SimulationAuthenticationError as exc:
        print(f"Simulation recorder authentication failed: {exc}")
        raise

    print("Simulation recorder authenticated successfully")

    with db_connect() as conn:
        for index, school_id in enumerate(SIMULATION_SCHOOL_IDS):
            failure = run_school(conn, request_url, school_id, access_token)
            if failure is not None:
                failures.append((school_id, failure))

            is_last_school = index == len(SIMULATION_SCHOOL_IDS) - 1
            if not is_last_school and SIMULATION_BETWEEN_SCHOOLS_DELAY_SECONDS > 0:
                print(
                    "Waiting before next school simulation: "
                    f"delay_seconds={SIMULATION_BETWEEN_SCHOOLS_DELAY_SECONDS}"
                )
                time.sleep(SIMULATION_BETWEEN_SCHOOLS_DELAY_SECONDS)

    if failures:
        failed_school_ids = ", ".join(school_id for school_id, _ in failures)
        raise RuntimeError(f"Day-ahead simulations failed for: {failed_school_ids}")

    print("Simulation recorder completed for all configured schools")


if __name__ == "__main__":
    try:
        run()
    except Exception:
        sys.exit(1)
