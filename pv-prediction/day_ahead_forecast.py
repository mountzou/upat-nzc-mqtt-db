"""Day-ahead hourly PV forecast: Open-Meteo weather + engineered features + pickled model."""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import warnings
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from fetch_open_meteo_forecast import (
    DEFAULT_LAT,
    DEFAULT_LON,
    build_forecast_url,
    fetch_forecast_json,
    validate_hourly_payload,
)

DIR = Path(__file__).resolve().parent
MODEL_PATH = DIR / "pv_forecasting_model.pkl"
FEATURES_PATH = DIR / "pv_features.pkl"

# Below this global horizontal irradiance (W/m²), treat the hour as dark and force 0 kW.
DEFAULT_NIGHT_GHI_THRESHOLD_WM2 = 20.0


def parse_env_bool(env_var: str, default: bool = False) -> bool:
    raw = os.environ.get(env_var)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def load_feature_columns(model: object, features_path: Path) -> list[str]:
    if features_path.exists():
        cols = joblib.load(features_path)
        if isinstance(cols, np.ndarray):
            return [str(x) for x in cols.tolist()]
        if isinstance(cols, (list, tuple)):
            return [str(x) for x in cols]
        raise TypeError(f"pv_features.pkl must be a list or array, got {type(cols)}")
    if hasattr(model, "feature_names_in_") and model.feature_names_in_ is not None:
        return [str(x) for x in model.feature_names_in_]
    if hasattr(model, "get_booster"):
        fn = model.get_booster().feature_names
        if fn:
            return [str(x) for x in fn]
    raise FileNotFoundError(
        "Could not determine feature columns. Place pv_features.pkl next to the model "
        "(column names in prediction order), or train the estimator with feature names."
    )


def fetch_hourly_payload(
    *,
    latitude: float = DEFAULT_LAT,
    longitude: float = DEFAULT_LON,
    forecast_days: int = 2,
) -> dict:
    url = build_forecast_url(
        latitude=latitude,
        longitude=longitude,
        forecast_days=forecast_days,
    )
    data = fetch_forecast_json(url)
    validate_hourly_payload(data)
    return data["hourly"]


def engineer_features(forecast_df: pd.DataFrame, *, lag_1h_kw: float) -> pd.DataFrame:
    """Add only derived columns needed for inference (see pv_features.pkl)."""
    out = forecast_df.copy()
    out["timestamp"] = pd.to_datetime(out["time"])
    hour = out["timestamp"].dt.hour
    out["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    out["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    out["lag_1h"] = float(lag_1h_kw)
    # No reliable 1h-ago reading at local midnight for the forecast horizon → 0 by convention.
    out.loc[hour == 0, "lag_1h"] = 0.0
    return out


def slice_day_ahead_local(df: pd.DataFrame) -> pd.DataFrame:
    """Keep the second local calendar day in the series (D+1)."""
    if df.empty:
        raise ValueError("Forecast payload is empty; cannot build a day-ahead forecast")
    dates = sorted(df["timestamp"].dt.date.unique())
    if len(dates) < 2:
        raise ValueError(
            "Forecast payload does not include a second local date; "
            "increase --forecast-days to request day-ahead data"
        )
    target = dates[1]
    mask = df["timestamp"].dt.date == target
    return df.loc[mask].copy()


def parse_env_float(env_var: str, default: float) -> float:
    raw = os.environ.get(env_var)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as e:
        raise ValueError(f"{env_var} must be a float, got {raw!r}") from e


def optional_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def json_safe_value(value: object) -> object:
    if value is None or pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def parse_non_negative_env_float(env_var: str, default: float) -> float:
    value = parse_env_float(env_var, default)
    if value < 0:
        raise ValueError(f"{env_var} must be non-negative")
    return value


def resolve_lag_1h_kw(cli_lag_1h: float | None, cli_latest: float | None) -> float:
    """Telemetry for lag_1h: CLI overrides env; default 0.0 when unset."""
    if cli_lag_1h is not None:
        return float(cli_lag_1h)
    if cli_latest is not None:
        return float(cli_latest)
    latest_kw = parse_env_float("PV_LATEST_ACTIVE_POWER_KW", 0.0)
    return parse_env_float("PV_LAG_1H_KW", latest_kw)


def parse_night_ghi_threshold_wm2() -> float:
    return parse_non_negative_env_float(
        "PV_NIGHT_GHI_THRESHOLD_WM2",
        DEFAULT_NIGHT_GHI_THRESHOLD_WM2,
    )


def resolve_night_ghi_threshold_wm2(cli_threshold: float | None) -> float:
    if cli_threshold is None:
        return parse_night_ghi_threshold_wm2()
    if cli_threshold < 0:
        raise ValueError("--night-ghi-threshold must be non-negative")
    return float(cli_threshold)


def mask_power_below_ghi_threshold(
    day_df: pd.DataFrame,
    power_kw: np.ndarray,
    *,
    threshold_wm2: float,
) -> np.ndarray:
    """Zero kW when shortwave_radiation (GHI) is below threshold (night / no sun)."""
    sw = day_df["shortwave_radiation"].astype(float).to_numpy()
    out = np.asarray(power_kw, dtype=float).copy()
    out[sw < threshold_wm2] = 0.0
    return out


def run_forecast(
    *,
    lag_1h_kw: float,
    latitude: float = DEFAULT_LAT,
    longitude: float = DEFAULT_LON,
    forecast_days: int = 2,
    verbose: bool = False,
    apply_night_ghi_mask: bool = True,
    night_ghi_threshold_wm2: float | None = None,
) -> pd.DataFrame:
    if forecast_days < 2:
        raise ValueError("forecast_days must be at least 2 for a day-ahead forecast")
    hourly = fetch_hourly_payload(
        latitude=latitude,
        longitude=longitude,
        forecast_days=forecast_days,
    )
    raw_df = pd.DataFrame(hourly)
    full_df = engineer_features(raw_df, lag_1h_kw=lag_1h_kw)
    day_df = slice_day_ahead_local(full_df)

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, module="xgboost.core")
        model = joblib.load(MODEL_PATH)
        features = load_feature_columns(model, FEATURES_PATH)
    missing = [c for c in features if c not in day_df.columns]
    if missing:
        raise KeyError(
            f"Model expects columns not present after engineering: {missing}"
        )

    X = day_df[features]
    if verbose:
        print("Feature columns (order):", features, file=sys.stderr)
        print("lag_1h_kw (scalar before midnight override):", lag_1h_kw, file=sys.stderr)
        noon = day_df["timestamp"].dt.hour == 12
        if noon.any():
            print("Sample X (noon):\n", X.loc[noon].iloc[0], file=sys.stderr)
        midn = day_df["timestamp"].dt.hour == 0
        if midn.any():
            print("Sample X (midnight):\n", X.loc[midn].iloc[0], file=sys.stderr)

    raw_pred = model.predict(X)
    if verbose:
        print(
            "Raw predict min/max/mean:",
            float(np.min(raw_pred)),
            float(np.max(raw_pred)),
            float(np.mean(raw_pred)),
            file=sys.stderr,
        )

    if np.all(raw_pred <= 0.0):
        print(
            "WARNING: XGBoost returned non-positive values for every hour before clipping. "
            "Check lag_1h / PV_LAG_1H_KW / PV_LATEST_ACTIVE_POWER_KW and that the model "
            "matches pv_features.pkl.",
            file=sys.stderr,
        )

    day_df = day_df.copy()
    clipped = np.clip(raw_pred, a_min=0.0, a_max=None)
    if apply_night_ghi_mask:
        clipped = mask_power_below_ghi_threshold(
            day_df,
            clipped,
            threshold_wm2=resolve_night_ghi_threshold_wm2(night_ghi_threshold_wm2),
        )
    day_df["predicted_power_kw"] = clipped
    if verbose:
        day_df["predicted_power_kw_raw"] = raw_pred
    return day_df


def print_forecast(forecast_df: pd.DataFrame) -> None:
    print("\nDAY-AHEAD PV FORECAST\n")
    for _, row in forecast_df.iterrows():
        ts = row["timestamp"]
        power = row["predicted_power_kw"]
        print(f"{ts}: {power:.2f} kW")
    daily_energy = forecast_df["predicted_power_kw"].sum()
    print(f"\nPredicted daily energy: {daily_energy:.2f} kWh")


def db_connect():
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_INTERNAL_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def save_forecast_to_db(
    forecast_df: pd.DataFrame,
    *,
    latitude: float,
    longitude: float,
    forecast_days: int,
    lag_1h_kw: float,
    apply_night_ghi_mask: bool,
    night_ghi_threshold_wm2: float,
) -> int:
    from psycopg2.extras import Json

    if forecast_df.empty:
        raise ValueError("Cannot save an empty PV forecast")

    forecast_date = forecast_df["timestamp"].iloc[0].date()
    daily_energy_kwh = float(forecast_df["predicted_power_kw"].sum())
    raw_request = {
        "latitude": latitude,
        "longitude": longitude,
        "forecast_days": forecast_days,
        "lag_1h_kw": lag_1h_kw,
        "apply_night_ghi_mask": apply_night_ghi_mask,
        "night_ghi_threshold_wm2": night_ghi_threshold_wm2,
    }
    raw_summary = {
        "hourly_rows": int(len(forecast_df)),
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
                    latitude,
                    longitude,
                    forecast_days,
                    lag_1h_kw,
                    night_ghi_threshold_wm2,
                    daily_energy_kwh,
                    MODEL_PATH.name,
                    FEATURES_PATH.name,
                    Json(raw_request),
                    Json(raw_summary),
                ),
            )
            run_id = cur.fetchone()[0]

            for _, row in forecast_df.iterrows():
                ts = row["timestamp"]
                raw_features = {
                    key: json_safe_value(row.get(key))
                    for key in (
                        "shortwave_radiation",
                        "direct_normal_irradiance",
                        "diffuse_radiation",
                        "temperature_2m",
                        "cloud_cover",
                        "wind_speed_10m",
                        "hour_sin",
                        "hour_cos",
                        "lag_1h",
                    )
                }

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
                        optional_float(row.get("shortwave_radiation")),
                        optional_float(row.get("direct_normal_irradiance")),
                        optional_float(row.get("diffuse_radiation")),
                        optional_float(row.get("temperature_2m")),
                        optional_float(row.get("cloud_cover")),
                        optional_float(row.get("wind_speed_10m")),
                        optional_float(row.get("lag_1h")),
                        Json(raw_features),
                    ),
                )

    return run_id


def main() -> int:
    parser = argparse.ArgumentParser(description="Day-ahead PV forecast from Open-Meteo + XGBoost model.")
    parser.add_argument(
        "--latest-power-kw",
        type=float,
        default=None,
        help="Alias for --lag-1h-kw when the latter is omitted (measured kW ~1h ago).",
    )
    parser.add_argument(
        "--lag-1h-kw",
        type=float,
        default=None,
        help="Measured active power ~1h ago (kW); overrides PV_LAG_1H_KW. Hour 00:00 uses lag_1h=0.",
    )
    parser.add_argument(
        "--forecast-days",
        type=int,
        default=2,
        help="Open-Meteo forecast_days (use >=2 for D+1 in local timezone=auto).",
    )
    parser.add_argument(
        "--latitude",
        type=float,
        default=DEFAULT_LAT,
        help=f"Forecast latitude; default {DEFAULT_LAT}.",
    )
    parser.add_argument(
        "--longitude",
        type=float,
        default=DEFAULT_LON,
        help=f"Forecast longitude; default {DEFAULT_LON}.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print diagnostics (feature order, sample row, raw predict stats) to stderr",
    )
    parser.add_argument(
        "--no-night-ghi-mask",
        action="store_true",
        help="Do not zero predictions when shortwave_radiation (GHI) is very low",
    )
    parser.add_argument(
        "--night-ghi-threshold",
        type=float,
        default=None,
        metavar="W_M2",
        help=(
            "GHI (W/m²) below which predicted kW is forced to 0; "
            f"default {DEFAULT_NIGHT_GHI_THRESHOLD_WM2} or PV_NIGHT_GHI_THRESHOLD_WM2"
        ),
    )
    parser.add_argument(
        "--save-to-db",
        action="store_true",
        default=None,
        help="Persist the successful forecast to Postgres",
    )
    parser.add_argument(
        "--no-save-to-db",
        action="store_false",
        dest="save_to_db",
        help="Do not persist the forecast even if PV_SAVE_TO_DB=true",
    )
    args = parser.parse_args()

    try:
        lag_resolved = resolve_lag_1h_kw(args.lag_1h_kw, args.latest_power_kw)
        apply_night_ghi_mask = not args.no_night_ghi_mask
        night_ghi_threshold = resolve_night_ghi_threshold_wm2(args.night_ghi_threshold)
        out = run_forecast(
            lag_1h_kw=lag_resolved,
            latitude=args.latitude,
            longitude=args.longitude,
            forecast_days=args.forecast_days,
            verbose=args.verbose,
            apply_night_ghi_mask=apply_night_ghi_mask,
            night_ghi_threshold_wm2=night_ghi_threshold,
        )
    except (
        urllib.error.HTTPError,
        urllib.error.URLError,
        OSError,
        ValueError,
        KeyError,
        FileNotFoundError,
        TypeError,
        json.JSONDecodeError,
    ) as e:
        print(e, file=sys.stderr)
        return 1

    print_forecast(out)
    save_to_db = (
        args.save_to_db
        if args.save_to_db is not None
        else parse_env_bool("PV_SAVE_TO_DB", False)
    )
    if save_to_db:
        try:
            run_id = save_forecast_to_db(
                out,
                latitude=args.latitude,
                longitude=args.longitude,
                forecast_days=args.forecast_days,
                lag_1h_kw=lag_resolved,
                apply_night_ghi_mask=apply_night_ghi_mask,
                night_ghi_threshold_wm2=night_ghi_threshold,
            )
        except Exception as e:
            print(f"Failed to save PV forecast to database: {e}", file=sys.stderr)
            return 1
        print(f"\nSaved PV forecast run_id={run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
