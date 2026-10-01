"""Collect FusionSolar PV telemetry and store it in PostgreSQL."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

from fusionsolar import FusionSolarClient, FusionSolarError
from processing import (
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
from database import PersistenceError, persist_batch
from api_limits import ApiControl, ApiControlError


def connect_to_fusionsolar(control: ApiControl) -> FusionSolarClient:
    client = FusionSolarClient(
        base_url=os.environ["FUSIONSOLAR_BASE_URL"],
        username=os.environ["FUSIONSOLAR_USERNAME"],
        system_code=os.environ["FUSIONSOLAR_SYSTEM_CODE"],
        control=control,
    )
    client.login()
    return client


def fetch_pv_data(
    request_window: dict[str, Any],
) -> tuple[str, list[dict], dict, list[dict]]:
    # All requests share this durable ledger.
    with ApiControl(os.getenv("PV_API_STATE_DIR"),
                    os.environ["FUSIONSOLAR_USERNAME"]) as control:
        control.preflight(2)
        plant_code = os.environ["FUSIONSOLAR_PLANT_CODE"]
        client = connect_to_fusionsolar(control)
        devices = normalize_devices(client.get_device_list(plant_code))
        ids_by_type = device_ids_by_type(devices)

        history = {}
        for device_type in (INVERTER_DEVICE_TYPE, METER_DEVICE_TYPE):
            device_ids = ids_by_type.get(device_type, [])
            if device_ids:
                history[device_type] = client.get_history(
                    device_ids=device_ids,
                    device_type=device_type,
                    start_ms=request_window["start_ms"],
                    end_ms=request_window["end_ms"],
                )
        return plant_code, devices, history, client.call_reports


def run() -> dict[str, Any]:
    request_window = build_request_window(
        target_date=default_target_date(),
        lookback_days=3,
    )
    plant_code, devices, history, api_calls = fetch_pv_data(request_window)

    return build_ingestion_batch(
        plant_code=plant_code,
        request_window=request_window,
        devices=devices,
        history_by_device_type=history,
        api_calls=api_calls,
        collected_at=datetime.now(timezone.utc),
    )


def main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(description="Collect and store FusionSolar PV telemetry.").parse_args(argv)
    batch = run()
    batch["persistence"] = persist_batch(batch)
    print(json.dumps(batch_summary(batch), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (
        FusionSolarError,
        ApiControlError,
        PipelineValidationError,
        PersistenceError,
        OSError,
        ValueError,
    ) as exc:
        print(f"collect-pv failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
