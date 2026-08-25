"""Pure FusionSolar-to-persistence-batch transformation."""

from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo


PLANT_TIMEZONE_NAME = "Europe/Athens"
PLANT_TIMEZONE = ZoneInfo(PLANT_TIMEZONE_NAME)
INVERTER_DEVICE_TYPE = 1
METER_DEVICE_TYPE = 17
LOGGER_DEVICE_TYPE = 63

DEVICE_ROLES = {
    INVERTER_DEVICE_TYPE: "inverter",
    METER_DEVICE_TYPE: "grid_meter",
    LOGGER_DEVICE_TYPE: "logger",
}

TYPED_KPI_FIELDS = {
    "active_power": "active_power_kw",
    "reactive_power": "reactive_power_kvar",
    "mppt_power": "mppt_power_kw",
    "power_factor": "power_factor",
    "efficiency": "efficiency_percent",
    "temperature": "temperature_c",
    "elec_freq": "grid_frequency_hz",
    "inverter_state": "inverter_state",
    "day_cap": "day_energy_kwh",
    "total_cap": "total_energy_kwh",
}


class PipelineValidationError(ValueError):
    """The API payload cannot safely become a persistence batch."""


def default_target_date(now: datetime | None = None) -> date:
    current = now or datetime.now(tz=PLANT_TIMEZONE)
    if current.tzinfo is None:
        current = current.replace(tzinfo=PLANT_TIMEZONE)
    return current.astimezone(PLANT_TIMEZONE).date() - timedelta(days=1)


def build_request_window(
    *,
    target_date: date,
    lookback_days: int,
) -> dict[str, Any]:
    if not 1 <= lookback_days <= 3:
        raise ValueError("lookback_days must be between 1 and 3")
    start_date = target_date - timedelta(days=lookback_days - 1)
    start_local = datetime.combine(start_date, time.min, tzinfo=PLANT_TIMEZONE)
    end_exclusive_local = datetime.combine(
        target_date + timedelta(days=1),
        time.min,
        tzinfo=PLANT_TIMEZONE,
    )
    start_ms = int(start_local.timestamp() * 1000)
    end_ms = int(end_exclusive_local.timestamp() * 1000) - 1
    return {
        "start_date": start_date.isoformat(),
        "end_date": target_date.isoformat(),
        "lookback_days": lookback_days,
        "timezone": PLANT_TIMEZONE_NAME,
        "start_local": start_local.isoformat(),
        "end_exclusive_local": end_exclusive_local.isoformat(),
        "start_ms": start_ms,
        "end_ms": end_ms,
    }


def _successful_payload(payload: dict[str, Any], label: str) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise PipelineValidationError(f"{label} payload must be an object")
    if payload.get("success") is not True or payload.get("failCode") not in (0, None):
        raise PipelineValidationError(
            f"{label} payload is unsuccessful failCode={payload.get('failCode')}"
        )
    rows = payload.get("data") or []
    if not isinstance(rows, list):
        raise PipelineValidationError(f"{label}.data must be a list")
    return rows


def _device_id(value: Any, label: str) -> str:
    if value is None or isinstance(value, bool):
        raise PipelineValidationError(f"{label} is missing a device ID")
    return str(value)


def normalize_devices(device_list_payload: dict[str, Any]) -> list[dict[str, Any]]:
    devices = []
    seen_ids: set[str] = set()
    for raw in _successful_payload(device_list_payload, "getDevList"):
        if not isinstance(raw, dict):
            raise PipelineValidationError("getDevList returned a non-object device")
        provider_id = _device_id(raw.get("id"), "getDevList device")
        if provider_id in seen_ids:
            raise PipelineValidationError(
                f"getDevList returned duplicate device ID {provider_id}"
            )
        seen_ids.add(provider_id)
        device_type = raw.get("devTypeId")
        if isinstance(device_type, bool) or not isinstance(device_type, int):
            raise PipelineValidationError(
                f"device {provider_id} has invalid devTypeId={device_type!r}"
            )
        devices.append(
            {
                "provider_device_id": provider_id,
                "provider_device_dn": raw.get("devDn"),
                "name": raw.get("devName"),
                "device_type_code": device_type,
                "device_role": DEVICE_ROLES.get(device_type, "other"),
                "model": raw.get("invType") or raw.get("model"),
                "software_version": raw.get("softwareVersion"),
            }
        )
    if not devices:
        raise PipelineValidationError("getDevList returned no devices")
    return sorted(devices, key=lambda item: item["provider_device_id"])


def device_ids_by_type(devices: list[dict[str, Any]]) -> dict[int, list[str]]:
    grouped: dict[int, list[str]] = defaultdict(list)
    for device in devices:
        grouped[device["device_type_code"]].append(device["provider_device_id"])
    return {device_type: sorted(ids) for device_type, ids in grouped.items()}


def _typed_kpis(item_map: dict[str, Any], *, row_label: str) -> tuple[dict, dict]:
    typed: dict[str, int | float | None] = {}
    for source_key, target_key in TYPED_KPI_FIELDS.items():
        value = item_map.get(source_key)
        if value is None:
            typed[target_key] = None
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise PipelineValidationError(
                f"{row_label} has non-numeric {source_key}={value!r}"
            )
        typed[target_key] = (
            int(value) if source_key == "inverter_state" else float(value)
        )
    extra = {
        str(key): value
        for key, value in item_map.items()
        if key not in TYPED_KPI_FIELDS and value is not None
    }
    return typed, extra


def normalize_history(
    *,
    history_by_device_type: dict[int, dict[str, Any]],
    devices: list[dict[str, Any]],
    request_window: dict[str, Any],
    source_kind: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    device_by_id = {device["provider_device_id"]: device for device in devices}
    expected_by_type = device_ids_by_type(devices)
    normalized: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, int]] = set()
    warnings: list[str] = []

    for device_type in (INVERTER_DEVICE_TYPE, METER_DEVICE_TYPE):
        expected_ids = set(expected_by_type.get(device_type, []))
        payload = history_by_device_type.get(device_type)
        if payload is None:
            if device_type == INVERTER_DEVICE_TYPE and expected_ids:
                raise PipelineValidationError("inverter history payload is missing")
            if device_type == METER_DEVICE_TYPE and expected_ids:
                warnings.append("grid meter history was not requested")
            continue

        rows = _successful_payload(
            payload,
            f"getDevHistoryKpi(devTypeId={device_type})",
        )
        for index, raw in enumerate(rows):
            if not isinstance(raw, dict):
                raise PipelineValidationError(
                    f"history type {device_type} row {index} is not an object"
                )
            provider_id = _device_id(raw.get("devId"), f"history row {index}")
            if provider_id not in expected_ids or provider_id not in device_by_id:
                raise PipelineValidationError(
                    f"history type {device_type} returned unexpected device {provider_id}"
                )
            collect_time = raw.get("collectTime")
            if isinstance(collect_time, bool) or not isinstance(collect_time, (int, float)):
                raise PipelineValidationError(
                    f"history device {provider_id} has invalid collectTime"
                )
            collect_time_ms = int(collect_time)
            if not request_window["start_ms"] <= collect_time_ms <= request_window["end_ms"]:
                raise PipelineValidationError(
                    f"history device {provider_id} timestamp is outside the requested window"
                )
            key = (provider_id, collect_time_ms)
            if key in seen_keys:
                raise PipelineValidationError(
                    f"duplicate history row for device {provider_id} at {collect_time_ms}"
                )
            seen_keys.add(key)

            observed_utc = datetime.fromtimestamp(
                collect_time_ms / 1000,
                tz=timezone.utc,
            )
            observed_local = observed_utc.astimezone(PLANT_TIMEZONE)
            if (
                observed_local.minute % 5 != 0
                or observed_local.second != 0
                or observed_local.microsecond != 0
            ):
                raise PipelineValidationError(
                    f"history device {provider_id} timestamp is outside the 5-minute grid"
                )
            item_map = raw.get("dataItemMap")
            if not isinstance(item_map, dict) or not item_map:
                raise PipelineValidationError(
                    f"history device {provider_id} has an empty dataItemMap"
                )
            typed, extra = _typed_kpis(
                item_map,
                row_label=f"device {provider_id} at {collect_time_ms}",
            )
            quality_flags = []
            if typed["active_power_kw"] is None:
                quality_flags.append("missing_active_power")
            normalized.append(
                {
                    "provider_device_id": provider_id,
                    "device_role": device_by_id[provider_id]["device_role"],
                    "observed_at": observed_utc.isoformat(),
                    "observed_at_local": observed_local.isoformat(),
                    "local_date": observed_local.date().isoformat(),
                    "provider_collect_time_ms": collect_time_ms,
                    **typed,
                    "extra_kpis": extra,
                    "quality_flags": quality_flags,
                    "source_kind": source_kind,
                }
            )

    counts = Counter(row["provider_device_id"] for row in normalized)
    missing_inverters = [
        device_id
        for device_id in expected_by_type.get(INVERTER_DEVICE_TYPE, [])
        if counts[device_id] == 0
    ]
    if missing_inverters:
        raise PipelineValidationError(
            "no historical readings returned for expected inverter(s): "
            + ", ".join(missing_inverters)
        )
    missing_meters = [
        device_id
        for device_id in expected_by_type.get(METER_DEVICE_TYPE, [])
        if counts[device_id] == 0
    ]
    if missing_meters:
        warnings.append(
            "no historical readings returned for grid meter(s): "
            + ", ".join(missing_meters)
        )
    return sorted(
        normalized,
        key=lambda row: (row["provider_collect_time_ms"], row["provider_device_id"]),
    ), warnings


def aggregate_plant_readings(
    *,
    device_readings: list[dict[str, Any]],
    devices: list[dict[str, Any]],
    source_kind: str,
) -> list[dict[str, Any]]:
    inverter_ids = {
        device["provider_device_id"]
        for device in devices
        if device["device_role"] == "inverter"
    }
    if not inverter_ids:
        raise PipelineValidationError("no inverter devices are available for aggregation")
    by_timestamp: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in device_readings:
        if row["provider_device_id"] in inverter_ids:
            by_timestamp[row["provider_collect_time_ms"]].append(row)

    plant_rows = []
    for collect_time_ms, rows in sorted(by_timestamp.items()):
        reporting_ids = {row["provider_device_id"] for row in rows}
        missing_ids = sorted(inverter_ids - reporting_ids)

        def aggregate_metric(field: str) -> tuple[float | None, int]:
            values = [row[field] for row in rows if row[field] is not None]
            return (sum(values), len(values)) if values else (None, 0)

        active_power, active_count = aggregate_metric("active_power_kw")
        reactive_power, reactive_count = aggregate_metric("reactive_power_kvar")
        mppt_power, mppt_count = aggregate_metric("mppt_power_kw")
        quality_flags = []
        if missing_ids:
            quality_flags.append("missing_inverter_reading")
        if active_count != len(inverter_ids):
            quality_flags.append("incomplete_active_power")
        if reactive_count != len(inverter_ids):
            quality_flags.append("incomplete_reactive_power")
        if mppt_count != len(inverter_ids):
            quality_flags.append("incomplete_mppt_power")

        power_factor = None
        if active_power is not None and reactive_power is not None:
            apparent_power = math.hypot(active_power, reactive_power)
            power_factor = active_power / apparent_power if apparent_power > 0 else None

        first = rows[0]
        plant_rows.append(
            {
                "observed_at": first["observed_at"],
                "observed_at_local": first["observed_at_local"],
                "local_date": first["local_date"],
                "provider_collect_time_ms": collect_time_ms,
                "active_power_kw": active_power,
                "reactive_power_kvar": reactive_power,
                "mppt_power_kw": mppt_power,
                "power_factor": power_factor,
                "reporting_device_count": len(reporting_ids),
                "expected_device_count": len(inverter_ids),
                "reporting_device_ids": sorted(reporting_ids),
                "missing_device_ids": missing_ids,
                "quality_status": "complete" if not quality_flags else "partial",
                "quality_flags": quality_flags,
                "source_kind": f"{source_kind}_device_derived",
            }
        )
    if not plant_rows:
        raise PipelineValidationError("no inverter readings are available for aggregation")
    return plant_rows


def build_ingestion_batch(
    *,
    plant_code: str,
    request_window: dict[str, Any],
    device_list_payload: dict[str, Any],
    history_by_device_type: dict[int, dict[str, Any]],
    source_kind: str,
    api_calls: list[dict[str, Any]] | None = None,
    collected_at: datetime | None = None,
) -> dict[str, Any]:
    if source_kind not in {"fusion_live", "fixture"}:
        raise ValueError("source_kind must be fusion_live or fixture")
    devices = normalize_devices(device_list_payload)
    device_readings, warnings = normalize_history(
        history_by_device_type=history_by_device_type,
        devices=devices,
        request_window=request_window,
        source_kind=source_kind,
    )
    plant_readings = aggregate_plant_readings(
        device_readings=device_readings,
        devices=devices,
        source_kind=source_kind,
    )
    per_device_counts = Counter(
        row["provider_device_id"] for row in device_readings
    )
    complete_count = sum(
        row["quality_status"] == "complete" for row in plant_readings
    )
    partial_count = len(plant_readings) - complete_count
    if partial_count:
        warnings.append(
            f"{partial_count} plant timestamp(s) have partial inverter coverage"
        )
    energy_estimate = sum(
        row["active_power_kw"] * (5 / 60)
        for row in plant_readings
        if row["active_power_kw"] is not None
    )
    collected = collected_at or datetime.now(timezone.utc)
    if collected.tzinfo is None:
        collected = collected.replace(tzinfo=timezone.utc)
    active_power_values = [
        row["active_power_kw"]
        for row in plant_readings
        if row["active_power_kw"] is not None
    ]
    batch = {
        "schema_version": "pv-ingestion-batch-v1",
        "run_key": (
            f"{source_kind}:{plant_code}:{request_window['start_date']}:"
            f"{request_window['end_date']}"
        ),
        "collected_at": collected.astimezone(timezone.utc).isoformat(),
        "source": "huawei_fusionsolar",
        "source_kind": source_kind,
        "plant": {
            "provider_plant_dn": plant_code,
            "timezone": PLANT_TIMEZONE_NAME,
        },
        "request_window": request_window,
        "api_calls": list(api_calls or []),
        "devices": devices,
        "device_readings": device_readings,
        "plant_readings": plant_readings,
        "quality": {
            "status": "complete" if not warnings else "partial",
            "warnings": warnings,
            "device_reading_count": len(device_readings),
            "plant_reading_count": len(plant_readings),
            "complete_plant_timestamp_count": complete_count,
            "partial_plant_timestamp_count": partial_count,
            "per_device_reading_counts": dict(sorted(per_device_counts.items())),
            "observed_sample_energy_estimate_kwh": energy_estimate,
            "peak_active_power_kw": (
                max(active_power_values) if active_power_values else None
            ),
        },
        "persistence": {
            "status": "not_attempted",
            "adapter": None,
            "rows_written": 0,
        },
    }
    json.dumps(batch)
    return batch


def batch_summary(batch: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": batch["schema_version"],
        "run_key": batch["run_key"],
        "request_window": batch["request_window"],
        "api_call_count": len(batch["api_calls"]),
        "device_count": len(batch["devices"]),
        "device_reading_count": batch["quality"]["device_reading_count"],
        "plant_reading_count": batch["quality"]["plant_reading_count"],
        "quality": batch["quality"],
        "persistence": batch["persistence"],
    }
