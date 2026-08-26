import os
import sys
import time
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from urllib.parse import urlencode
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

OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_LATITUDE = float(os.getenv("OPEN_METEO_LATITUDE", "37.068"))
OPEN_METEO_LONGITUDE = float(os.getenv("OPEN_METEO_LONGITUDE", "22.026"))
OPEN_METEO_TIMEZONE = os.getenv("OPEN_METEO_TIMEZONE", "Europe/Athens")
OPEN_METEO_WIND_SPEED_UNIT = os.getenv("OPEN_METEO_WIND_SPEED_UNIT", "ms")
OPEN_METEO_FORECAST_DAYS = int(os.getenv("OPEN_METEO_FORECAST_DAYS", "8"))
OPEN_METEO_TIMEOUT_SECONDS = float(os.getenv("OPEN_METEO_TIMEOUT_SECONDS", "30"))

HOURLY_VARIABLES = [
    "temperature_2m",
    "dew_point_2m",
    "relative_humidity_2m",
    "surface_pressure",
    "shortwave_radiation",
    "direct_normal_irradiance",
    "diffuse_radiation",
    "wind_direction_10m",
    "wind_speed_10m",
    "weather_code",
    "snow_depth",
    "precipitation",
    "cloud_cover",
]


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
                f"(attempt {attempt}/{DB_CONNECT_RETRIES}): {exc}",
                file=sys.stderr,
            )
            if attempt < DB_CONNECT_RETRIES:
                time.sleep(DB_CONNECT_DELAY_SECONDS)

    raise connection_error


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


def get_forecast_dates():
    today = datetime.now(ZoneInfo(OPEN_METEO_TIMEZONE)).date()
    end_date = today + timedelta(days=OPEN_METEO_FORECAST_DAYS - 1)
    return today, end_date


def build_forecast_request():
    start_date, end_date = get_forecast_dates()
    params = {
        "latitude": OPEN_METEO_LATITUDE,
        "longitude": OPEN_METEO_LONGITUDE,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": OPEN_METEO_TIMEZONE,
        "wind_speed_unit": OPEN_METEO_WIND_SPEED_UNIT,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    return params, f"{OPEN_METEO_FORECAST_URL}?{urlencode(params)}"


def fetch_forecast_json(url):
    response = requests.get(
        url,
        headers={"User-Agent": "weather-collector/1.0"},
        timeout=OPEN_METEO_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def validate_hourly_payload(data):
    hourly = data.get("hourly")
    if not isinstance(hourly, dict):
        raise ValueError("Open-Meteo response missing hourly object")

    times = hourly.get("time")
    if not times:
        raise ValueError("Open-Meteo response missing hourly.time")

    for variable in HOURLY_VARIABLES:
        values = hourly.get(variable)
        if values is None:
            raise ValueError(f"Open-Meteo response missing hourly.{variable}")
        if not isinstance(values, list):
            raise ValueError(
                f"Open-Meteo response hourly.{variable} must be an array"
            )
        if len(values) != len(times):
            raise ValueError(
                f"Open-Meteo length mismatch for {variable}: {len(values)} vs {len(times)}"
            )


def iter_hourly_rows(data, request_params):
    hourly = data["hourly"]
    times = hourly["time"]

    for idx, timestamp_text in enumerate(times):
        timestamp = datetime.fromisoformat(timestamp_text)
        raw_values = {
            variable: hourly[variable][idx]
            for variable in HOURLY_VARIABLES
        }
        row = {
            "source": "open-meteo",
            "latitude": Decimal(str(OPEN_METEO_LATITUDE)),
            "longitude": Decimal(str(OPEN_METEO_LONGITUDE)),
            "timezone": OPEN_METEO_TIMEZONE,
            "forecast_timestamp": timestamp,
            "forecast_date": timestamp.date(),
            "forecast_hour": timestamp.hour,
            "raw_values": raw_values,
            "raw_request": request_params,
        }

        for variable in HOURLY_VARIABLES:
            value = raw_values.get(variable)
            if variable == "weather_code":
                row[variable] = int_or_none(value)
            else:
                row[variable] = decimal_or_none(value)

        yield row


def save_hourly_rows(conn, rows):
    columns = [
        "source",
        "latitude",
        "longitude",
        "timezone",
        "forecast_timestamp",
        "forecast_date",
        "forecast_hour",
        *HOURLY_VARIABLES,
        "raw_values",
        "raw_request",
    ]

    update_columns = [
        column
        for column in columns
        if column not in {"source", "latitude", "longitude", "forecast_timestamp"}
    ]

    placeholders = ", ".join(["%s"] * len(columns))
    column_sql = ", ".join(columns)
    update_sql = ",\n                        ".join(
        f"{column} = EXCLUDED.{column}" for column in update_columns
    )

    query = f"""
        INSERT INTO weather_hourly_forecasts ({column_sql})
        VALUES ({placeholders})
        ON CONFLICT (source, latitude, longitude, forecast_timestamp)
        DO UPDATE SET
                        {update_sql},
                        fetched_at = NOW(),
                        updated_at = NOW();
    """

    count = 0
    with conn.cursor() as cur:
        for row in rows:
            values = [
                Json(row[column]) if column in {"raw_values", "raw_request"} else row[column]
                for column in columns
            ]
            cur.execute(query, values)
            count += 1

    return count


def main():
    request_params, url = build_forecast_request()
    print(f"GET {url}")

    data = fetch_forecast_json(url)
    validate_hourly_payload(data)

    rows = list(iter_hourly_rows(data, request_params))
    with db_connect() as conn:
        saved_count = save_hourly_rows(conn, rows)

    print(
        "Saved Open-Meteo hourly forecast rows: "
        f"{saved_count} ({request_params['start_date']}..{request_params['end_date']})"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (requests.RequestException, psycopg2.Error, ValueError) as exc:
        print(f"weather-collector failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
