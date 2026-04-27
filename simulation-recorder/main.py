import json
import os
import sys
import time
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import psycopg2
import requests
from psycopg2.extras import Json


DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_INTERNAL_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_CONNECT_RETRIES = int(os.getenv("POSTGRES_CONNECT_RETRIES", "5"))
DB_CONNECT_DELAY_SECONDS = float(os.getenv("POSTGRES_CONNECT_DELAY_SECONDS", "2"))

SIMULATION_API_BASE_URL = "https://upat-nzc-energyplus-backend.onrender.com"
SIMULATION_API_PATH = "/simulate/day-ahead"
SIMULATION_SCHOOL_ID = "school_10"
SIMULATION_REQUEST_TIMEOUT_SECONDS = 60
SIMULATION_REQUEST_RETRIES = 3
SIMULATION_REQUEST_RETRY_DELAY_SECONDS = 5
SIMULATION_RECORDING_TIMEZONE = "Europe/Athens"
SIMULATION_REQUEST_BODY = {
    "school_id": SIMULATION_SCHOOL_ID,
}
LOCAL_TZ = ZoneInfo(SIMULATION_RECORDING_TIMEZONE)


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


def build_simulation_url():
    base_url = SIMULATION_API_BASE_URL.rstrip("/") + "/"
    path = SIMULATION_API_PATH.lstrip("/")
    return urljoin(base_url, path)


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


def finish_failed_day_ahead_run(conn, run_id, http_status, response_json, error_text):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE simulation_day_ahead_runs
            SET
                completed_at = %s,
                http_status = %s,
                success = FALSE,
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


def fetch_simulation_response(request_url):
    last_error = None

    for attempt in range(1, SIMULATION_REQUEST_RETRIES + 1):
        try:
            return requests.post(
                request_url,
                json=SIMULATION_REQUEST_BODY,
                timeout=SIMULATION_REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            last_error = exc
            print(
                "Simulation request failed "
                f"(attempt {attempt}/{SIMULATION_REQUEST_RETRIES}): {exc}"
            )
            if attempt < SIMULATION_REQUEST_RETRIES:
                time.sleep(SIMULATION_REQUEST_RETRY_DELAY_SECONDS)

    raise last_error


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


def run():
    request_url = build_simulation_url()
    started_at = utc_now()
    recording_date = started_at.astimezone(LOCAL_TZ).date()
    run_id = None
    failure = None

    print("Starting simulation recorder")
    print(f"SIMULATION_API_BASE_URL={SIMULATION_API_BASE_URL}")
    print(f"SIMULATION_API_PATH={SIMULATION_API_PATH}")
    print(f"SIMULATION_REQUEST_BODY={json.dumps(SIMULATION_REQUEST_BODY, sort_keys=True)}")

    with db_connect() as conn:
        run_id = create_day_ahead_run(
            conn,
            SIMULATION_SCHOOL_ID,
            recording_date,
            request_url,
            SIMULATION_API_PATH,
            SIMULATION_REQUEST_BODY,
            started_at,
        )

        response_json = None
        http_status = None

        try:
            response = fetch_simulation_response(request_url)
            http_status = response.status_code
            try:
                response_json = response.json()
            except ValueError:
                response_json = None

            if not response.ok:
                raise requests.HTTPError(
                    f"Simulation API returned HTTP {http_status}",
                    response=response,
                )

            if response_json is None:
                raise ValueError("Simulation response is not valid JSON")

            room_results = extract_room_results(response_json)
            finish_successful_day_ahead_run(conn, run_id, http_status, response_json)
            result_count = insert_day_ahead_room_results(
                conn,
                run_id,
                SIMULATION_SCHOOL_ID,
                recording_date,
                room_results,
            )
            print(
                "Simulation recorder completed: "
                f"run_id={run_id}, room_results={result_count}"
            )
        except (ValueError, json.JSONDecodeError, requests.RequestException) as exc:
            finish_failed_day_ahead_run(conn, run_id, http_status, response_json, str(exc))
            print(f"Simulation recorder failed: run_id={run_id}, error={exc}")
            failure = exc

    if failure is not None:
        raise failure


if __name__ == "__main__":
    try:
        run()
    except Exception:
        sys.exit(1)
