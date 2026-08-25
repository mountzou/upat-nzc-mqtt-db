"""Day-ahead hourly PV forecast: Open-Meteo weather + engineered features + pickled model."""
from __future__ import annotations

import argparse
import math
import os
import sys
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
MODEL_PATH = DIR / "pv_forecasting_model_rf_operational_20260806.pkl"
FEATURES_PATH = DIR / "pv_features_rf_operational_20260806.pkl"

# Below this global horizontal irradiance (W/m²), treat the hour as dark and force 0 kW.
DEFAULT_NIGHT_GHI_THRESHOLD_WM2 = 20.0

PERSISTED_RAW_FEATURE_COLUMNS = (
    "temperature_2m",
    "shortwave_radiation",
    "direct_normal_irradiance",
    "diffuse_radiation",
    "cloud_cover",
    "wind_speed_10m",
    "hour_sin",
    "hour_cos",
    "doy_sin",
    "doy_cos",
    # Retained for schema compatibility; the current RF excludes it.
    "lag_1h",
)


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
        raise TypeError(
            f"{features_path.name} must be a list or array, got {type(cols)}"
        )
    if hasattr(model, "feature_names_in_") and model.feature_names_in_ is not None:
        return [str(x) for x in model.feature_names_in_]
    raise FileNotFoundError(
        f"Could not determine feature columns. Provide {features_path.name} "
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


def engineer_features(
    forecast_df: pd.DataFrame,
    *,
    lag_1h_kw: float | None,
) -> pd.DataFrame:
    """Add derived columns used by the tracked model and persisted provenance."""
    out = forecast_df.copy()
    out["timestamp"] = pd.to_datetime(out["time"])
    hour = out["timestamp"].dt.hour
    day_of_year = out["timestamp"].dt.dayofyear
    out["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    out["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    out["doy_sin"] = np.sin(2 * np.pi * day_of_year / 365.25)
    out["doy_cos"] = np.cos(2 * np.pi * day_of_year / 365.25)
    lag_1h_kw = normalize_optional_legacy_float(lag_1h_kw, label="lag_1h_kw")
    # Keep an explicitly supplied legacy value for schema/CLI compatibility. It
    # is not part of the current Random Forest feature contract. Missing legacy
    # metadata stays NULL in persistence instead of becoming a synthetic zero.
    if lag_1h_kw is None:
        out["lag_1h"] = np.nan
    else:
        out["lag_1h"] = float(lag_1h_kw)
        # Preserve the historical convention for explicitly supplied metadata.
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


def parse_optional_env_float(env_var: str) -> float | None:
    raw = os.environ.get(env_var)
    if raw is None or raw == "":
        return None
    try:
        value = float(raw)
    except ValueError as e:
        raise ValueError(f"{env_var} must be a float, got {raw!r}") from e
    return normalize_optional_legacy_float(value, label=env_var)


def normalize_optional_legacy_float(
    value: object,
    *,
    label: str,
) -> float | None:
    """Map missing/NaN legacy metadata to NULL and reject infinities."""
    if value is None:
        return None
    numeric = float(value)
    if math.isnan(numeric):
        return None
    if not math.isfinite(numeric):
        raise ValueError(f"{label} must be finite or empty, got {value!r}")
    return numeric


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


def build_raw_features(row) -> dict[str, object]:
    """Return persisted feature provenance for the current RF plus legacy lag."""
    return {
        key: json_safe_value(row.get(key))
        for key in PERSISTED_RAW_FEATURE_COLUMNS
    }


def parse_non_negative_env_float(env_var: str, default: float) -> float:
    value = parse_env_float(env_var, default)
    if value < 0:
        raise ValueError(f"{env_var} must be non-negative")
    return value


def resolve_lag_1h_kw(
    cli_lag_1h: float | None,
    cli_latest: float | None,
) -> float | None:
    """Resolve optional legacy metadata retained for schema/CLI compatibility."""
    if cli_lag_1h is not None:
        normalized = normalize_optional_legacy_float(
            cli_lag_1h,
            label="--lag-1h-kw",
        )
        if normalized is not None:
            return normalized
    if cli_latest is not None:
        normalized = normalize_optional_legacy_float(
            cli_latest,
            label="--latest-power-kw",
        )
        if normalized is not None:
            return normalized
    env_lag_1h = parse_optional_env_float("PV_LAG_1H_KW")
    if env_lag_1h is not None:
        return env_lag_1h
    return parse_optional_env_float("PV_LATEST_ACTIVE_POWER_KW")


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
    lag_1h_kw: float | None,
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
        print(
            "legacy lag_1h_kw metadata (not an RF input):",
            lag_1h_kw,
            file=sys.stderr,
        )
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
            "WARNING: The loaded model returned non-positive values for every hour "
            f"before clipping. Check the weather inputs and that {MODEL_PATH.name} "
            f"matches {FEATURES_PATH.name}.",
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
    lag_1h_kw: float | None,
    apply_night_ghi_mask: bool,
    night_ghi_threshold_wm2: float,
) -> int:
    from psycopg2.extras import Json

    if forecast_df.empty:
        raise ValueError("Cannot save an empty PV forecast")

    lag_1h_kw = normalize_optional_legacy_float(lag_1h_kw, label="lag_1h_kw")
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
        "hourly_rows": len(forecast_df),
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
                raw_features = build_raw_features(row)

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
    parser = argparse.ArgumentParser(
        description="Day-ahead PV forecast from Open-Meteo + tracked Random Forest model."
    )
    parser.add_argument(
        "--latest-power-kw",
        type=float,
        default=None,
        help=(
            "Legacy alias for --lag-1h-kw. Retained as persisted metadata; "
            "the current Random Forest does not use it for inference."
        ),
    )
    parser.add_argument(
        "--lag-1h-kw",
        type=float,
        default=None,
        help=(
            "Legacy measured-power metadata; overrides PV_LAG_1H_KW. "
            "The current Random Forest does not use it for inference."
        ),
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
        OSError,
        ValueError,
        KeyError,
        TypeError,
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
