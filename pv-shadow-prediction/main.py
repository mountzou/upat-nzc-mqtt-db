"""Run an isolated PV shadow candidate from one persisted champion snapshot.

This job never calls the weather provider and never mutates the champion tables.
Its default mode is read-only; ``--save-to-db`` is required to persist shadow rows.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import joblib
import numpy as np
import pandas as pd


DIR = Path(__file__).resolve().parent
DEFAULT_REGISTRY_PATH = DIR / "model_registry.json"
DEFAULT_ARTIFACT_DIR = DIR.parent / "pv-prediction"
DEFAULT_FREEZE_PATH = DEFAULT_ARTIFACT_DIR / "pv_champion_freeze_v1.json"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise TypeError(f"{path} must contain a JSON object")
    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def require_sha256(value: object, label: str) -> str:
    text = str(value)
    if not SHA256_RE.fullmatch(text):
        raise ValueError(f"{label} must be a lowercase SHA-256 digest")
    return text


def candidate_spec(registry: dict[str, Any], candidate_id: str) -> dict[str, Any]:
    if registry.get("schema_version") != "pv-shadow-registry.v1":
        raise ValueError("Unsupported shadow registry schema")
    candidates = registry.get("candidates")
    if not isinstance(candidates, dict) or candidate_id not in candidates:
        raise KeyError(f"Unknown shadow candidate: {candidate_id}")
    spec = candidates[candidate_id]
    if not isinstance(spec, dict):
        raise TypeError(f"Candidate {candidate_id} must be a JSON object")
    return spec


def artifact_paths(spec: dict[str, Any], artifact_dir: Path) -> dict[str, Path]:
    return {
        "model": artifact_dir / str(spec["model_artifact"]),
        "features": artifact_dir / str(spec["features_artifact"]),
        "manifest": artifact_dir / str(spec["manifest_artifact"]),
    }


def verify_candidate_artifacts(
    spec: dict[str, Any],
    baseline: dict[str, Any],
    artifact_dir: Path,
) -> tuple[object, list[str], dict[str, str]]:
    paths = artifact_paths(spec, artifact_dir)
    expected = {
        "model": require_sha256(spec["model_sha256"], "model_sha256"),
        "features": require_sha256(spec["features_sha256"], "features_sha256"),
        "manifest": require_sha256(spec["manifest_sha256"], "manifest_sha256"),
    }
    actual = {name: sha256_file(path) for name, path in paths.items()}
    if actual != expected:
        raise ValueError(f"Candidate artifact hash mismatch: expected={expected}, actual={actual}")

    features_value = joblib.load(paths["features"])
    if isinstance(features_value, np.ndarray):
        features = [str(value) for value in features_value.tolist()]
    elif isinstance(features_value, (list, tuple)):
        features = [str(value) for value in features_value]
    else:
        raise TypeError("Feature artifact must contain a list or array")

    manifest = load_json(paths["manifest"])
    if manifest.get("feature_contract") != features:
        raise ValueError("Feature artifact order does not match the model manifest")

    if spec.get("baseline_control"):
        champion = baseline.get("champion", {})
        frozen = {
            "model": champion.get("model", {}).get("sha256"),
            "features": champion.get("features", {}).get("sha256"),
            "manifest": champion.get("training_manifest", {}).get("sha256"),
        }
        if frozen != expected:
            raise ValueError("Replay artifacts do not match the frozen champion")
        if manifest.get("model_version") != champion.get("model_version"):
            raise ValueError("Replay model version does not match the frozen champion")

    model = joblib.load(paths["model"])
    model_features = getattr(model, "feature_names_in_", None)
    if model_features is not None and [str(value) for value in model_features] != features:
        raise ValueError("Estimator feature order does not match the feature artifact")
    return model, features, actual


def db_connect():
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_INTERNAL_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def expected_day_ahead_date() -> date:
    return datetime.now(ZoneInfo("Europe/Athens")).date() + timedelta(days=1)


def fetch_champion_snapshot(source_run_id: int | None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    from psycopg2.extras import RealDictCursor

    with db_connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if source_run_id is None:
                cur.execute(
                    """
                    SELECT
                        id,
                        forecast_date,
                        model_artifact,
                        features_artifact,
                        night_ghi_threshold_wm2,
                        raw_request,
                        started_at
                    FROM pv_day_ahead_forecast_runs
                    WHERE success = TRUE AND forecast_date = %s
                    ORDER BY started_at DESC
                    LIMIT 1;
                    """,
                    (expected_day_ahead_date(),),
                )
            else:
                cur.execute(
                    """
                    SELECT
                        id,
                        forecast_date,
                        model_artifact,
                        features_artifact,
                        night_ghi_threshold_wm2,
                        raw_request,
                        started_at
                    FROM pv_day_ahead_forecast_runs
                    WHERE id = %s AND success = TRUE;
                    """,
                    (source_run_id,),
                )
            run = cur.fetchone()
            if run is None:
                raise LookupError("No successful champion D+1 run found")

            cur.execute(
                """
                SELECT
                    run_id AS source_champion_run_id,
                    forecast_timestamp,
                    forecast_date,
                    forecast_hour,
                    predicted_power_kw AS champion_power_kw,
                    raw_features
                FROM pv_day_ahead_forecast_hourly
                WHERE run_id = %s
                ORDER BY forecast_timestamp ASC;
                """,
                (run["id"],),
            )
            rows = list(cur.fetchall())
    return dict(run), [dict(row) for row in rows]


def validate_champion_snapshot(
    run: dict[str, Any],
    rows: list[dict[str, Any]],
    baseline: dict[str, Any],
    required_features: list[str],
) -> None:
    champion = baseline.get("champion", {})
    if run.get("model_artifact") != champion.get("model", {}).get("path"):
        raise ValueError("Source run is not the frozen champion model artifact")
    if run.get("features_artifact") != champion.get("features", {}).get("path"):
        raise ValueError("Source run is not the frozen champion feature artifact")
    inference_contract = baseline.get("inference_contract", {})
    frozen_threshold = float(inference_contract["night_ghi_threshold_wm2"])
    source_threshold = float(run["night_ghi_threshold_wm2"])
    if not math.isclose(source_threshold, frozen_threshold, abs_tol=1e-12):
        raise ValueError("Source run uses a different night GHI threshold")
    raw_request = run.get("raw_request")
    if not isinstance(raw_request, dict) or raw_request.get("apply_night_ghi_mask") is not True:
        raise ValueError("Source run does not prove that the champion night mask was enabled")
    if len(rows) != 24:
        raise ValueError(f"Champion run must contain 24 rows, found {len(rows)}")

    hours = [int(row["forecast_hour"]) for row in rows]
    if hours != list(range(24)):
        raise ValueError(f"Champion hours must be ordered 0..23, found {hours}")
    timestamps = [row["forecast_timestamp"] for row in rows]
    if len(set(timestamps)) != 24:
        raise ValueError("Champion forecast timestamps must be unique")

    for row in rows:
        if row["source_champion_run_id"] != run["id"]:
            raise ValueError("Champion hourly row belongs to another run")
        if row["forecast_date"] != run["forecast_date"]:
            raise ValueError("Champion hourly row has a mismatched forecast date")
        raw_features = row.get("raw_features")
        if not isinstance(raw_features, dict):
            raise ValueError("Champion hourly row is missing raw_features")
        missing = [name for name in required_features if raw_features.get(name) is None]
        if missing:
            raise ValueError(f"Champion hourly row is missing candidate features: {missing}")


def materialize_deterministic_features(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Rebuild only timestamp-derived features omitted by older persistence code."""
    materialized: list[dict[str, Any]] = []
    reconstructed: set[str] = set()
    for source in rows:
        row = dict(source)
        features = dict(row.get("raw_features") or {})
        timestamp = row["forecast_timestamp"]
        hour = int(timestamp.hour)
        day_of_year = int(timestamp.timetuple().tm_yday)
        derived = {
            "hour_sin": math.sin(2 * math.pi * hour / 24),
            "hour_cos": math.cos(2 * math.pi * hour / 24),
            "doy_sin": math.sin(2 * math.pi * day_of_year / 365.25),
            "doy_cos": math.cos(2 * math.pi * day_of_year / 365.25),
        }
        for name, value in derived.items():
            if features.get(name) is None:
                features[name] = value
                reconstructed.add(name)
        row["raw_features"] = features
        materialized.append(row)
    return materialized, sorted(reconstructed)


def _json_default(value: object) -> object:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Cannot JSON encode {type(value).__name__}")


def snapshot_sha256(rows: list[dict[str, Any]]) -> str:
    payload = [
        {
            "forecast_timestamp": row["forecast_timestamp"],
            "forecast_hour": int(row["forecast_hour"]),
            "raw_features": row["raw_features"],
        }
        for row in rows
    ]
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def predict_candidate(
    model: object,
    features: list[str],
    rows: list[dict[str, Any]],
    night_ghi_threshold_wm2: float,
) -> list[dict[str, Any]]:
    frame = pd.DataFrame([row["raw_features"] for row in rows], columns=features)
    frame = frame.apply(pd.to_numeric, errors="raise")
    if frame.isna().any().any():
        raise ValueError("Candidate feature frame contains null values")

    raw = np.asarray(model.predict(frame), dtype=float)
    if raw.shape != (24,) or not np.isfinite(raw).all():
        raise ValueError("Candidate must produce 24 finite predictions")
    final = np.clip(raw, a_min=0.0, a_max=None)
    ghi = frame["shortwave_radiation"].to_numpy(dtype=float)
    final[ghi < float(night_ghi_threshold_wm2)] = 0.0

    return [
        {
            "source_champion_run_id": row["source_champion_run_id"],
            "forecast_timestamp": row["forecast_timestamp"],
            "forecast_date": row["forecast_date"],
            "forecast_hour": int(row["forecast_hour"]),
            "predicted_power_kw_raw": float(raw[index]),
            "predicted_power_kw": float(final[index]),
        }
        for index, row in enumerate(rows)
    ]


def validate_control_replay(
    predictions: list[dict[str, Any]],
    source_rows: list[dict[str, Any]],
    *,
    tolerance_kw: float = 1e-8,
) -> None:
    if len(predictions) != 24 or len(source_rows) != 24:
        raise ValueError("Champion replay requires 24 paired rows")
    differences = [
        abs(
            float(prediction["predicted_power_kw"])
            - float(source["champion_power_kw"])
        )
        for prediction, source in zip(predictions, source_rows)
    ]
    if max(differences) > tolerance_kw:
        raise ValueError(
            "Champion replay does not reproduce the persisted champion prediction"
        )


def start_shadow_run(
    run: dict[str, Any],
    candidate_id: str,
    spec: dict[str, Any],
    hashes: dict[str, str],
    input_sha256: str,
) -> tuple[int, bool]:
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    status,
                    model_sha256,
                    features_sha256,
                    manifest_sha256,
                    input_sha256,
                    (
                        SELECT COUNT(*)
                        FROM pv_shadow_forecast_hourly
                        WHERE shadow_run_id = pv_shadow_forecast_runs.id
                    ) AS hourly_rows
                FROM pv_shadow_forecast_runs
                WHERE source_champion_run_id = %s
                  AND candidate_id = %s
                  AND candidate_version = %s
                FOR UPDATE;
                """,
                (run["id"], candidate_id, spec["candidate_version"]),
            )
            existing = cur.fetchone()
            if existing and existing[1] == "success":
                existing_hashes = tuple(existing[2:6])
                expected_hashes = (
                    hashes["model"],
                    hashes["features"],
                    hashes["manifest"],
                    input_sha256,
                )
                if existing_hashes != expected_hashes:
                    raise ValueError(
                        "Existing successful shadow run has different provenance"
                    )
                if int(existing[6]) != 24:
                    raise ValueError(
                        "Existing successful shadow run does not contain 24 rows"
                    )
                return int(existing[0]), True

            params = (
                run["id"],
                run["forecast_date"],
                candidate_id,
                spec["candidate_version"],
                spec["model_artifact"],
                hashes["model"],
                spec["features_artifact"],
                hashes["features"],
                spec["manifest_artifact"],
                hashes["manifest"],
                input_sha256,
            )
            if existing:
                cur.execute(
                    """
                    UPDATE pv_shadow_forecast_runs
                    SET forecast_date = %s,
                        model_artifact = %s,
                        model_sha256 = %s,
                        features_artifact = %s,
                        features_sha256 = %s,
                        manifest_artifact = %s,
                        manifest_sha256 = %s,
                        input_sha256 = %s,
                        status = 'running',
                        daily_energy_kwh = NULL,
                        error_text = NULL,
                        started_at = NOW(),
                        completed_at = NULL,
                        updated_at = NOW()
                    WHERE id = %s;
                    """,
                    (
                        run["forecast_date"],
                        spec["model_artifact"],
                        hashes["model"],
                        spec["features_artifact"],
                        hashes["features"],
                        spec["manifest_artifact"],
                        hashes["manifest"],
                        input_sha256,
                        existing[0],
                    ),
                )
                return int(existing[0]), False

            cur.execute(
                """
                INSERT INTO pv_shadow_forecast_runs (
                    source_champion_run_id,
                    forecast_date,
                    candidate_id,
                    candidate_version,
                    model_artifact,
                    model_sha256,
                    features_artifact,
                    features_sha256,
                    manifest_artifact,
                    manifest_sha256,
                    input_sha256,
                    status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'running')
                RETURNING id;
                """,
                params,
            )
            return int(cur.fetchone()[0]), False


def save_shadow_success(shadow_run_id: int, predictions: list[dict[str, Any]]) -> None:
    if len(predictions) != 24:
        raise ValueError("A successful shadow run must contain 24 predictions")
    daily_energy_kwh = sum(row["predicted_power_kw"] for row in predictions)
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM pv_shadow_forecast_hourly WHERE shadow_run_id = %s;",
                (shadow_run_id,),
            )
            for row in predictions:
                cur.execute(
                    """
                    INSERT INTO pv_shadow_forecast_hourly (
                        shadow_run_id,
                        source_champion_run_id,
                        forecast_timestamp,
                        forecast_date,
                        forecast_hour,
                        predicted_power_kw_raw,
                        predicted_power_kw
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        shadow_run_id,
                        row["source_champion_run_id"],
                        row["forecast_timestamp"],
                        row["forecast_date"],
                        row["forecast_hour"],
                        row["predicted_power_kw_raw"],
                        row["predicted_power_kw"],
                    ),
                )
            cur.execute(
                """
                UPDATE pv_shadow_forecast_runs
                SET status = 'success',
                    daily_energy_kwh = %s,
                    error_text = NULL,
                    completed_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s;
                """,
                (daily_energy_kwh, shadow_run_id),
            )


def mark_shadow_failed(shadow_run_id: int, error: Exception) -> None:
    error_text = f"{type(error).__name__}: {error}"[:1000]
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pv_shadow_forecast_runs
                SET status = 'failed',
                    daily_energy_kwh = NULL,
                    error_text = %s,
                    completed_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s;
                """,
                (error_text, shadow_run_id),
            )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--candidate-id",
        default=os.getenv("PV_SHADOW_CANDIDATE_ID", "champion-replay"),
    )
    parser.add_argument("--source-run-id", type=int, default=None)
    save_group = parser.add_mutually_exclusive_group()
    save_group.add_argument("--save-to-db", action="store_true", dest="save_to_db")
    save_group.add_argument("--no-save-to-db", action="store_false", dest="save_to_db")
    parser.set_defaults(save_to_db=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    registry_path = Path(os.getenv("PV_SHADOW_REGISTRY_PATH", DEFAULT_REGISTRY_PATH))
    artifact_dir = Path(os.getenv("PV_SHADOW_ARTIFACT_DIR", DEFAULT_ARTIFACT_DIR))
    freeze_path = Path(os.getenv("PV_CHAMPION_FREEZE_PATH", DEFAULT_FREEZE_PATH))
    save_to_db = (
        args.save_to_db
        if args.save_to_db is not None
        else env_bool("PV_SHADOW_SAVE_TO_DB", False)
    )
    shadow_run_id: int | None = None

    try:
        registry = load_json(registry_path)
        baseline = load_json(freeze_path)
        spec = candidate_spec(registry, args.candidate_id)
        model, features, hashes = verify_candidate_artifacts(spec, baseline, artifact_dir)
        run, rows = fetch_champion_snapshot(args.source_run_id)
        rows, reconstructed_features = materialize_deterministic_features(rows)
        validate_champion_snapshot(run, rows, baseline, features)
        input_sha256 = snapshot_sha256(rows)

        already_complete = False
        if save_to_db:
            shadow_run_id, already_complete = start_shadow_run(
                run,
                args.candidate_id,
                spec,
                hashes,
                input_sha256,
            )
        if already_complete:
            print(f"Shadow run already complete: id={shadow_run_id}")
            return 0

        predictions = predict_candidate(
            model,
            features,
            rows,
            float(spec["night_ghi_threshold_wm2"]),
        )
        if spec.get("baseline_control"):
            validate_control_replay(predictions, rows)
        daily_energy_kwh = sum(row["predicted_power_kw"] for row in predictions)
        if save_to_db:
            assert shadow_run_id is not None
            save_shadow_success(shadow_run_id, predictions)
        print(
            f"candidate={args.candidate_id} version={spec['candidate_version']} "
            f"source_run_id={run['id']} forecast_date={run['forecast_date']} "
            f"rows=24 daily_energy_kwh={daily_energy_kwh:.6f} "
            f"input_sha256={input_sha256} "
            f"reconstructed_features={','.join(reconstructed_features) or 'none'} "
            f"saved={save_to_db}"
        )
        return 0
    except Exception as error:
        if save_to_db and shadow_run_id is not None:
            try:
                mark_shadow_failed(shadow_run_id, error)
            except Exception as mark_error:
                print(f"Failed to record shadow error: {mark_error}", file=sys.stderr)
        print(f"Shadow forecast failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
