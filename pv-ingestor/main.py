"""Build a validated PV ingestion batch and optionally persist a live run."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from fusionsolar import FusionSolarClient, FusionSolarError
from pipeline import (
    INVERTER_DEVICE_TYPE,
    METER_DEVICE_TYPE,
    PipelineValidationError,
    batch_summary,
    build_ingestion_batch,
    build_request_window,
    default_target_date,
    device_ids_by_type,
    normalize_devices,
)
from persistence import PersistenceError, persist_batch


def _positive_lookback(value: str) -> int:
    parsed = int(value)
    if not 1 <= parsed <= 3:
        raise argparse.ArgumentTypeError("lookback must be between 1 and 3 days")
    return parsed


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"required environment variable {name} is missing")
    return value


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch and post-process FusionSolar telemetry into a validated, "
            "persistence-ready batch. PostgreSQL writes are explicit opt-in."
        )
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--live",
        action="store_true",
        help="perform the explicitly requested sequential FusionSolar calls",
    )
    mode.add_argument(
        "--fixture",
        type=Path,
        help="read recorded API payloads instead of contacting FusionSolar",
    )
    parser.add_argument(
        "--target-date",
        type=date.fromisoformat,
        help="last completed Europe/Athens date; defaults to yesterday",
    )
    parser.add_argument(
        "--lookback-days",
        type=_positive_lookback,
        default=_positive_lookback(os.getenv("FUSIONSOLAR_LOOKBACK_DAYS", "3")),
        help="inclusive rolling history window, from 1 to 3 days",
    )
    parser.add_argument(
        "--skip-meter",
        action="store_true",
        default=not _env_bool("FUSIONSOLAR_INCLUDE_METER", True),
        help="skip the separate grid-meter history call",
    )
    parser.add_argument(
        "--emit-json",
        action="store_true",
        help="emit the full persistence-ready batch to stdout",
    )
    save_mode = parser.add_mutually_exclusive_group()
    save_mode.add_argument(
        "--save-to-db",
        action="store_true",
        dest="save_to_db",
        help="persist a live batch to PostgreSQL in one transaction",
    )
    save_mode.add_argument(
        "--no-save-to-db",
        action="store_false",
        dest="save_to_db",
        help="disable persistence even if PV_INGESTOR_SAVE_TO_DB=true",
    )
    parser.set_defaults(save_to_db=None)
    parser.add_argument(
        "--site-key",
        default=os.getenv("PV_SITE_KEY", ""),
        help="stable local site identifier; required only with --save-to-db",
    )
    parser.add_argument(
        "--trigger-kind",
        choices=("scheduled", "manual", "backfill", "migration"),
        default=os.getenv("PV_INGESTOR_TRIGGER_KIND", "manual"),
        help="audit provenance for a persisted ingestion run",
    )
    return parser.parse_args(argv)


def _load_fixture(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        fixture = json.load(handle)
    if not isinstance(fixture, dict):
        raise ValueError("fixture root must be a JSON object")
    return fixture


def _fixture_inputs(
    fixture: dict[str, Any],
    args: argparse.Namespace,
) -> tuple[str, dict, dict, list[dict]]:
    plant_code = str(fixture.get("plant_code") or "").strip()
    if not plant_code:
        raise ValueError("fixture plant_code is required")
    if args.target_date is None:
        target_text = fixture.get("target_date")
        if not target_text:
            raise ValueError("fixture target_date is required")
        args.target_date = date.fromisoformat(str(target_text))
    history_raw = fixture.get("history_by_device_type") or {}
    history = {int(key): value for key, value in history_raw.items()}
    if args.skip_meter:
        history.pop(METER_DEVICE_TYPE, None)
    return (
        plant_code,
        fixture.get("device_list") or {},
        history,
        list(fixture.get("api_calls") or []),
    )


def _live_inputs(
    args: argparse.Namespace,
    request_window: dict[str, Any],
) -> tuple[str, dict, dict, list[dict]]:
    plant_code = _required_env("FUSIONSOLAR_PLANT_CODE")
    client = FusionSolarClient(
        base_url=_required_env("FUSIONSOLAR_BASE_URL"),
        username=_required_env("FUSIONSOLAR_USERNAME"),
        system_code=_required_env("FUSIONSOLAR_SYSTEM_CODE"),
    )
    client.login()
    device_list_payload = client.get_device_list(plant_code)
    devices = normalize_devices(device_list_payload)
    ids_by_type = device_ids_by_type(devices)
    inverter_ids = ids_by_type.get(INVERTER_DEVICE_TYPE, [])
    if not inverter_ids:
        raise PipelineValidationError("getDevList returned no inverters")

    history = {
        INVERTER_DEVICE_TYPE: client.get_history(
            device_ids=inverter_ids,
            device_type=INVERTER_DEVICE_TYPE,
            start_ms=request_window["start_ms"],
            end_ms=request_window["end_ms"],
        )
    }
    meter_ids = ids_by_type.get(METER_DEVICE_TYPE, [])
    if meter_ids and not args.skip_meter:
        history[METER_DEVICE_TYPE] = client.get_history(
            device_ids=meter_ids,
            device_type=METER_DEVICE_TYPE,
            start_ms=request_window["start_ms"],
            end_ms=request_window["end_ms"],
        )
    return plant_code, device_list_payload, history, client.call_reports


def run(args: argparse.Namespace) -> dict[str, Any]:
    fixture = _load_fixture(args.fixture) if args.fixture else None
    if fixture is not None:
        plant_code, device_payload, history, api_calls = _fixture_inputs(
            fixture,
            args,
        )
    else:
        plant_code = _required_env("FUSIONSOLAR_PLANT_CODE")
        device_payload = {}
        history = {}
        api_calls = []

    target_date = args.target_date or default_target_date()
    request_window = build_request_window(
        target_date=target_date,
        lookback_days=args.lookback_days,
    )
    if fixture is None:
        plant_code, device_payload, history, api_calls = _live_inputs(
            args,
            request_window,
        )

    return build_ingestion_batch(
        plant_code=plant_code,
        request_window=request_window,
        device_list_payload=device_payload,
        history_by_device_type=history,
        source_kind="fixture" if fixture is not None else "fusion_live",
        api_calls=api_calls,
        collected_at=datetime.now(timezone.utc),
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    batch = run(args)
    save_to_db = (
        args.save_to_db
        if args.save_to_db is not None
        else _env_bool("PV_INGESTOR_SAVE_TO_DB", False)
    )
    if save_to_db:
        if args.fixture is not None:
            raise ValueError("fixture mode cannot persist to PostgreSQL")
        batch["persistence"] = persist_batch(
            batch,
            site_key=args.site_key,
            trigger_kind=args.trigger_kind,
            code_version=os.getenv("PV_INGESTOR_CODE_VERSION") or None,
        )
    output = batch if args.emit_json else batch_summary(batch)
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (
        FusionSolarError,
        PipelineValidationError,
        PersistenceError,
        OSError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        print(f"pv-ingestor failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
