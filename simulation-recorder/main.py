import json
import os
import sys
import time
from datetime import datetime, timezone
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
SIMULATION_API_PATH = "/rooms"
SIMULATION_SCHOOL_ID = "school_10"
SIMULATION_REQUEST_TIMEOUT_SECONDS = 60
SIMULATION_REQUEST_RETRIES = 3
SIMULATION_REQUEST_RETRY_DELAY_SECONDS = 5
SIMULATION_RECORDING_TIMEZONE = "Europe/Athens"
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


def bool_or_none(value):
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    return bool(value)


def create_run(conn, school_id, request_url, request_path, started_at):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO simulation_runs (
                school_id,
                request_url,
                request_path,
                started_at,
                success
            )
            VALUES (%s, %s, %s, %s, FALSE)
            RETURNING id;
            """,
            (school_id, request_url, request_path, started_at),
        )
        return cur.fetchone()[0]


def finish_run(conn, run_id, http_status, success, response_json, error_text):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE simulation_runs
            SET
                completed_at = %s,
                http_status = %s,
                success = %s,
                response_json = %s,
                error_text = %s
            WHERE id = %s;
            """,
            (
                utc_now(),
                http_status,
                success,
                Json(response_json) if response_json is not None else None,
                error_text,
                run_id,
            ),
        )


def fetch_simulation_response(request_url):
    last_error = None

    for attempt in range(1, SIMULATION_REQUEST_RETRIES + 1):
        try:
            return requests.get(
                request_url,
                params={"school_id": SIMULATION_SCHOOL_ID},
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


def extract_items(response_json):
    if isinstance(response_json, list):
        return response_json

    if isinstance(response_json, dict):
        for key in ("items", "rooms", "outputs"):
            value = response_json.get(key)
            if isinstance(value, list):
                return value

    raise ValueError("Simulation response must be a list or contain items, rooms, or outputs")


def insert_recordings(conn, run_id, school_id, recording_date, items):
    inserted_count = 0

    with conn.cursor() as cur:
        for item in items:
            if not isinstance(item, dict):
                raise ValueError("Simulation response items must be objects")

            room_id = item.get("id") or item.get("room_id")
            if not room_id:
                raise ValueError("Simulation response item is missing id or room_id")

            supports = item.get("supports") if isinstance(item.get("supports"), dict) else {}
            defaults = item.get("defaults") if isinstance(item.get("defaults"), dict) else {}

            cur.execute(
                """
                INSERT INTO simulation_room_recordings (
                    run_id,
                    school_id,
                    recording_date,
                    room_id,
                    label,
                    physical_instance_count,
                    idf_file,
                    zone_name,
                    thermostat_type,
                    supports_cooling_setpoint,
                    default_occupancy,
                    default_heating_setpoint,
                    default_cooling_setpoint,
                    default_lighting_w_per_m2,
                    default_infiltration_ach,
                    raw_item
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (school_id, recording_date, room_id)
                DO UPDATE SET
                    run_id = EXCLUDED.run_id,
                    label = EXCLUDED.label,
                    physical_instance_count = EXCLUDED.physical_instance_count,
                    idf_file = EXCLUDED.idf_file,
                    zone_name = EXCLUDED.zone_name,
                    thermostat_type = EXCLUDED.thermostat_type,
                    supports_cooling_setpoint = EXCLUDED.supports_cooling_setpoint,
                    default_occupancy = EXCLUDED.default_occupancy,
                    default_heating_setpoint = EXCLUDED.default_heating_setpoint,
                    default_cooling_setpoint = EXCLUDED.default_cooling_setpoint,
                    default_lighting_w_per_m2 = EXCLUDED.default_lighting_w_per_m2,
                    default_infiltration_ach = EXCLUDED.default_infiltration_ach,
                    raw_item = EXCLUDED.raw_item,
                    updated_at = NOW();
                """,
                (
                    run_id,
                    school_id,
                    recording_date,
                    room_id,
                    item.get("label"),
                    int_or_none(item.get("physical_instance_count")),
                    item.get("idf_file"),
                    item.get("zone_name"),
                    item.get("thermostat_type"),
                    bool_or_none(supports.get("cooling_setpoint")),
                    int_or_none(defaults.get("occupancy")),
                    decimal_or_none(defaults.get("heating_setpoint")),
                    decimal_or_none(defaults.get("cooling_setpoint")),
                    decimal_or_none(defaults.get("lighting_w_per_m2")),
                    decimal_or_none(defaults.get("infiltration_ach")),
                    Json(item),
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
    print(f"SIMULATION_SCHOOL_ID={SIMULATION_SCHOOL_ID}")

    with db_connect() as conn:
        run_id = create_run(
            conn,
            SIMULATION_SCHOOL_ID,
            request_url,
            SIMULATION_API_PATH,
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

            items = extract_items(response_json)
            item_count = insert_recordings(
                conn,
                run_id,
                SIMULATION_SCHOOL_ID,
                recording_date,
                items,
            )
            finish_run(conn, run_id, http_status, True, response_json, None)
            print(
                "Simulation recorder completed: "
                f"run_id={run_id}, recordings={item_count}"
            )
        except (ValueError, json.JSONDecodeError, requests.RequestException) as exc:
            finish_run(conn, run_id, http_status, False, response_json, str(exc))
            print(f"Simulation recorder failed: run_id={run_id}, error={exc}")
            failure = exc

    if failure is not None:
        raise failure


if __name__ == "__main__":
    try:
        run()
    except Exception:
        sys.exit(1)
