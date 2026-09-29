from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import joblib
import numpy as np
import pandas as pd
import psycopg2

DEFAULT_LAT = float(os.getenv("OPEN_METEO_LATITUDE", "37.068"))
DEFAULT_LON = float(os.getenv("OPEN_METEO_LONGITUDE", "22.026"))
WEATHER_TIMEZONE = os.getenv("OPEN_METEO_TIMEZONE", "Europe/Athens")
WEATHER_MAX_AGE_HOURS = 24
CLIMATIC_VARIABLES = (
    "temperature_2m",
    "shortwave_radiation",
    "direct_normal_irradiance",
    "diffuse_radiation",
    "cloud_cover",
    "wind_speed_10m",
)

DIR = Path(__file__).resolve().parent
MODEL_PATH = DIR / "model.pkl"
MODEL_MANIFEST_PATH = DIR / "model_manifest.json"

# Below this global horizontal irradiance (W/m²), treat the hour as dark and force 0 kW.
NIGHT_GHI_THRESHOLD_WM2 = 20.0


def get_weather_forecast() -> pd.DataFrame:
    now = datetime.now(timezone.utc)
    target_date = now.astimezone(ZoneInfo(WEATHER_TIMEZONE)).date() + timedelta(days=1)

    with closing(db_connect()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT forecast_timestamp AS time,
                       temperature_2m, shortwave_radiation,
                       direct_normal_irradiance, diffuse_radiation, cloud_cover,
                       wind_speed_10m * 3.6 AS wind_speed_10m,
                       fetched_at
                FROM weather_hourly_forecasts
                WHERE source = 'open-meteo'
                  AND timezone = %s AND forecast_date = %s
                ORDER BY forecast_timestamp
                """,
                (WEATHER_TIMEZONE, target_date),
            )
            rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=["time", *CLIMATIC_VARIABLES, "fetched_at"])
    # The weather table stores one row per naive local hour (00:00 through 23:00).
    expected = pd.date_range(target_date, periods=24, freq="h")
    if list(pd.to_datetime(df["time"])) != list(expected):
        raise ValueError(f"Incomplete weather forecast for {target_date}: expected hours 00..23")
    fetched_at = pd.to_datetime(df["fetched_at"], utc=True)
    if not fetched_at.between(now - timedelta(hours=WEATHER_MAX_AGE_HOURS), now).all():
        raise ValueError("Weather forecast is stale or has an invalid fetched_at")
    df[list(CLIMATIC_VARIABLES)] = df[list(CLIMATIC_VARIABLES)].astype(float)
    if not np.isfinite(df[list(CLIMATIC_VARIABLES)].to_numpy()).all():
        raise ValueError("Weather forecast contains missing or non-finite values")
    return df.drop(columns=["fetched_at"])


def prepare_forecast_features(df_weather: pd.DataFrame) -> pd.DataFrame:
    df = df_weather.copy()
    df["time"] = pd.to_datetime(df["time"])
    hour, doy = df["time"].dt.hour, df["time"].dt.dayofyear
    df["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    df["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    df["doy_sin"] = np.sin(2 * np.pi * doy / 365.25)
    df["doy_cos"] = np.cos(2 * np.pi * doy / 365.25)
    return df


def run_forecast() -> pd.DataFrame:
    df_weather = get_weather_forecast()
    df_forecast = prepare_forecast_features(df_weather)

    model = joblib.load(MODEL_PATH)
    features = list(model.feature_names_in_)
    missing = [c for c in features if c not in df_forecast.columns]
    if missing:
        raise KeyError(f"Model expects columns not present after engineering: {missing}")

    df_features = df_forecast[features]
    raw_pred = model.predict(df_features)

    df_forecast["predicted_power_kw"] = np.clip(raw_pred, 0, None)
    df_forecast.loc[
        df_forecast["shortwave_radiation"] < NIGHT_GHI_THRESHOLD_WM2,
        "predicted_power_kw",
    ] = 0.0
    df_forecast["raw_features"] = df_features.astype(float).to_dict(orient="records")
    return df_forecast


def print_forecast(df_forecast: pd.DataFrame, *, verbose: bool = False) -> None:
    if verbose:
        for _, row in df_forecast.iterrows():
            print(f"{row['time']}: {row['predicted_power_kw']:.2f} kW")
    forecast_date = df_forecast["time"].iloc[0].date()
    daily_energy_kwh = df_forecast["predicted_power_kw"].sum()
    print(f"PV forecast: date={forecast_date}, hours={len(df_forecast)}, energy={daily_energy_kwh:.2f} kWh")


def db_connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_INTERNAL_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def save_forecast_to_db(df_forecast: pd.DataFrame) -> int:
    from psycopg2.extras import Json

    if df_forecast.empty:
        raise ValueError("Cannot save an empty PV forecast")

    forecast_date = df_forecast["time"].iloc[0].date()
    daily_energy_kwh = float(df_forecast["predicted_power_kw"].sum())
    raw_request = {
        "model_version": json.loads(MODEL_MANIFEST_PATH.read_text())["model_version"],
        "latitude": DEFAULT_LAT,
        "longitude": DEFAULT_LON,
        "apply_night_ghi_mask": True,
        "night_ghi_threshold_wm2": NIGHT_GHI_THRESHOLD_WM2,
    }
    raw_summary = {
        "hourly_rows": len(df_forecast),
        "daily_energy_kwh": daily_energy_kwh,
    }

    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pv_day_ahead_forecast_runs (
                    forecast_date,
                    latitude,
                    longitude,
                    forecast_days,
                    lag_1h_kw,
                    night_ghi_threshold_wm2,
                    daily_energy_kwh,
                    source,
                    model_artifact,
                    features_artifact,
                    success,
                    error_text,
                    raw_request,
                    raw_summary,
                    completed_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    'open-meteo',
                    %s, %s,
                    TRUE,
                    NULL,
                    %s,
                    %s,
                    NOW()
                )
                RETURNING id;
                """,
                (
                    forecast_date,
                    DEFAULT_LAT,
                    DEFAULT_LON,
                    2,  # Retain the existing D+1 horizon metadata in the run schema.
                    None,  # Unused legacy database column.
                    NIGHT_GHI_THRESHOLD_WM2,
                    daily_energy_kwh,
                    MODEL_PATH.name,
                    None,  # Feature names are embedded in the model artifact.
                    Json(raw_request),
                    Json(raw_summary),
                ),
            )
            run_id = cur.fetchone()[0]

            for _, row in df_forecast.iterrows():
                ts = row["time"]
                raw_features = row["raw_features"]

                cur.execute(
                    """
                    INSERT INTO pv_day_ahead_forecast_hourly (
                        run_id,
                        forecast_timestamp,
                        forecast_date,
                        forecast_hour,
                        predicted_power_kw,
                        shortwave_radiation_w_m2,
                        direct_normal_irradiance_w_m2,
                        diffuse_radiation_w_m2,
                        temperature_2m_c,
                        cloud_cover_percent,
                        wind_speed_10m,
                        lag_1h_kw,
                        raw_features
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s
                    );
                    """,
                    (
                        run_id,
                        ts.to_pydatetime(),
                        ts.date(),
                        int(ts.hour),
                        float(row["predicted_power_kw"]),
                        float(row["shortwave_radiation"]),
                        float(row["direct_normal_irradiance"]),
                        float(row["diffuse_radiation"]),
                        float(row["temperature_2m"]),
                        float(row["cloud_cover"]),
                        float(row["wind_speed_10m"]),
                        None,  # Unused legacy database column.
                        Json(raw_features),
                    ),
                )

    return run_id


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Day-ahead PV forecast from stored weather + tracked Random Forest model."
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print hourly predictions in addition to the daily summary",
    )
    parser.add_argument(
        "--no-save-to-db",
        action="store_true",
        help="Calculate and display the forecast without saving it to Postgres",
    )
    args = parser.parse_args()

    try:
        df_forecast = run_forecast()
    except (OSError, psycopg2.Error, ValueError, KeyError, TypeError) as e:
        print(e, file=sys.stderr)
        return 1

    print_forecast(df_forecast, verbose=args.verbose)
    if not args.no_save_to_db:
        try:
            run_id = save_forecast_to_db(df_forecast)
        except Exception as e:
            print(f"Failed to save PV forecast to database: {e}", file=sys.stderr)
            return 1
        print(f"Saved PV forecast run_id={run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
