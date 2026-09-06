from monitoring.local_data import local_read
import logging
from datetime import datetime, timedelta

from fastapi import HTTPException

from monitoring.config import (
    DEVICE_API_BASE_URL,
    ENERGY_CO2_FACTOR_KG_PER_KWH,
    ENERGY_CO2_FACTOR_REFERENCE_YEAR,
    ENERGY_CO2_FACTOR_SOURCE,
    ENERGY_CO2_FACTOR_VERSION,
)
from monitoring.policies.school_hours import is_school_hour_in_school_timezone
from monitoring.services.energy_demand.devices import is_whole_building_energy_meter
from monitoring.utils.api_datetime import get_history_window
from monitoring.utils.api_datetime import normalize_api_window_dt
from monitoring.utils.timezone import utc_now_naive

logger = logging.getLogger(__name__)

BASE_URL = DEVICE_API_BASE_URL.rstrip("/")


def _emissions_factor_contract() -> dict:
    return {
        "emissions_factor_kg_per_kwh": ENERGY_CO2_FACTOR_KG_PER_KWH,
        "emissions_factor_source": ENERGY_CO2_FACTOR_SOURCE,
        "emissions_factor_reference_year": ENERGY_CO2_FACTOR_REFERENCE_YEAR,
        "emissions_factor_version": ENERGY_CO2_FACTOR_VERSION,
    }


def _serialize_shelly_energy_window_param(dt: datetime) -> str:
    """
    Shelly /shelly/device/{id}/energy query validation:
    accepts YYYY-MM-DD (whole calendar days) or YYYY-MM-DDTHH:MM only — not seconds/subseconds.
    """
    floored = dt.replace(second=0, microsecond=0)
    return floored.strftime("%Y-%m-%dT%H:%M")


def _get_shelly_json_params(
    url: str,
    params: list[tuple[str, str | bool]] | dict | None,
    *,
    log_context: str,
):
    return local_read(url, params)


def _get_json(url: str, *, device_id: str, params: dict | None = None):
    return local_read(url, params)


def fetch_energy_device_latest(device_id: str, *, limit: int):
    url = f"{BASE_URL}/shelly/device/{device_id}/latest"
    return _get_json(url, device_id=device_id, params={"limit": limit})


def fetch_energy_device_history(
    device_id: str,
    *,
    aggregate: str,
    bucket_unit: str,
    bucket_size: int,
    limit: int,
    metrics: tuple[str, ...] | None = None,
):
    url = f"{BASE_URL}/shelly/device/{device_id}/history"
    start, end = get_history_window(
        bucket_unit=bucket_unit,
        bucket_size=bucket_size,
        limit=limit,
    )
    params = {
        "start": normalize_api_window_dt(start).isoformat(timespec="minutes"),
        "end": normalize_api_window_dt(end).isoformat(timespec="minutes"),
        "aggregate": aggregate,
        "bucket_unit": bucket_unit,
        "bucket_size": bucket_size,
    }
    if metrics:
        params["metric"] = metrics

    return _get_json(
        url,
        device_id=device_id,
        params=params,
    )


def fetch_shelly_pro3em_phase_energy_estimate(
    device_id: str,
    *,
    start: datetime,
    end: datetime,
    bucket_minutes: int = 30,
):
    """
    Call the upstream Shelly Pro 3EM-optimized energy endpoint (bucketed phase power → Wh).

    This mirrors GET /shelly/device/{device_id}/energy on the device API service.
    """
    start_naive = normalize_api_window_dt(start)
    end_naive = normalize_api_window_dt(end)
    if end_naive - start_naive > timedelta(days=90):
        raise HTTPException(400, "Energy history must not exceed 90 days")
    if end_naive < start_naive:
        raise HTTPException(status_code=400, detail="end must be greater than or equal to start")

    url = f"{BASE_URL}/shelly/device/{device_id}/energy"
    return _get_json(
        url,
        device_id=device_id,
        params={
            "start": _serialize_shelly_energy_window_param(start_naive),
            "end": _serialize_shelly_energy_window_param(end_naive),
            "bucket_minutes": bucket_minutes,
        },
    )


def default_completed_hour_window_utc(*, hours: int = 24) -> tuple[datetime, datetime]:
    """End = start of current UTC hour (completed hours only); start = end - hours."""
    now = utc_now_naive().replace(second=0, microsecond=0)
    end = now.replace(minute=0)
    start = end - timedelta(hours=hours)
    return start, end


def _device_display_label(device: dict) -> str:
    lab = device.get("label")
    if isinstance(lab, str) and lab.strip():
        return lab.strip()
    did = device.get("id")
    return did if isinstance(did, str) else "device"


def _hourly_wh_slice(energy_wh: dict, phase: str | None) -> float | None:
    """Read Wh from upstream energy_wh dict: total for plugs, phase key a|b|c for Pro 3EM."""
    key = "total" if phase is None else phase
    raw = energy_wh.get(key)
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


WHOLE_BUILDING_ROOM_KEY = "all_rooms"


def _school_has_whole_building_meter(devices: list[dict]) -> bool:
    return any(is_whole_building_energy_meter(device) for device in devices)


def build_school_wide_hourly_series_plan(
    room_devices: list[dict],
) -> list[tuple[str, str | None, str]]:
    """One total-energy series per device (plugs + whole Pro 3EM meters)."""
    plan: list[tuple[str, str | None, str]] = []
    for device in room_devices:
        device_id = device.get("id")
        if not isinstance(device_id, str) or not device_id:
            continue
        base_label = (
            device.get("label") if isinstance(device.get("label"), str) else _device_display_label(device)
        )
        plan.append((device_id, None, base_label))
    return plan


def build_room_hourly_series_plan(room_devices: list[dict], room_key: str) -> list[tuple[str, str | None, str]]:
    """
    One chart series per plug, or per Pro 3EM phase whose target_room_id matches this room.
    Tuple: (shelly_device_id, phase or None, display label).
    """
    plan: list[tuple[str, str | None, str]] = []
    room_key_norm = str(room_key or "").strip()
    for d in room_devices:
        did = d.get("id")
        if not isinstance(did, str) or not did:
            continue
        base_label = d.get("label") if isinstance(d.get("label"), str) else _device_display_label(d)
        if d.get("type") == "three_phase_meter":
            phases = d.get("phases")
            if not isinstance(phases, dict):
                phases = {}
            phase_entries: list[tuple[str, str | None, str]] = []
            for pkey in ("a", "b", "c"):
                meta = phases.get(pkey)
                if not isinstance(meta, dict):
                    continue
                target = str(meta.get("target_room_id") or "").strip()
                if target != room_key_norm:
                    continue
                plab = meta.get("display_label")
                label = plab.strip() if isinstance(plab, str) and plab.strip() else f"{base_label} · L{pkey.upper()}"
                phase_entries.append((did, pkey, label))
            if phase_entries:
                plan.extend(phase_entries)
            elif not phases:
                plan.append((did, None, base_label))
        else:
            plan.append((did, None, base_label))
    return plan


def aggregate_shelly_hourly_items_by_window_and_device(
    items: list,
    *,
    series_plan: list[tuple[str, str | None, str]],
) -> list[dict]:
    """Group upstream hourly rows by time window; split Wh per chart series (plugs + Pro 3EM phases)."""
    if not series_plan:
        return []

    n_series = len(series_plan)
    nested: dict[tuple[str, str], list[float]] = {}

    for item in items:
        if not isinstance(item, dict):
            continue
        did = item.get("device_id")
        if not isinstance(did, str) or not did:
            continue
        ws = item.get("window_start")
        we = item.get("window_end")
        if ws is None or we is None:
            continue
        energy_wh = item.get("energy_wh")
        if not isinstance(energy_wh, dict):
            continue
        if energy_wh.get("total") is None and not any(energy_wh.get(p) is not None for p in ("a", "b", "c")):
            continue

        key = (str(ws), str(we))
        if key not in nested:
            nested[key] = [0.0] * n_series
        row = nested[key]

        for i, (plan_did, phase, _) in enumerate(series_plan):
            if plan_did != did:
                continue
            v = _hourly_wh_slice(energy_wh, phase)
            if v is None:
                continue
            row[i] += v

    result: list[dict] = []
    for ws, we in sorted(nested.keys(), key=lambda k: k[0]):
        counts = nested[(ws, we)]
        by_device: list[dict] = []
        energy_total = 0.0
        for i, (plan_did, phase, label) in enumerate(series_plan):
            wh = float(counts[i])
            energy_total += wh
            did_out = plan_did if phase is None else f"{plan_did}:phase-{phase}"
            entry: dict = {
                "device_id": did_out,
                "label": label,
                "energy_wh": wh,
            }
            if phase is not None:
                entry["phase"] = phase
            by_device.append(entry)
        result.append(
            {
                "window_start": ws,
                "window_end": we,
                "energy_wh_total": energy_total,
                "by_device": by_device,
            }
        )
    return result


def fetch_shelly_hourly_energy(
    device_ids: list[str],
    *,
    start: datetime,
    end: datetime,
    working_only: bool = False,
) -> dict:
    """
    GET /shelly/hourly-energy on the device API (precomputed hourly Wh per device).
    """
    if not device_ids:
        raise ValueError("fetch_shelly_hourly_energy requires at least one device_id")

    start_naive = normalize_api_window_dt(start).replace(second=0, microsecond=0)
    end_naive = normalize_api_window_dt(end).replace(second=0, microsecond=0)
    if end_naive - start_naive > timedelta(days=90):
        raise HTTPException(400, "Energy history must not exceed 90 days")
    if end_naive < start_naive:
        raise HTTPException(status_code=400, detail="end must be greater than or equal to start")

    params: list[tuple[str, str | bool]] = [("device_id", did) for did in device_ids]
    params.append(("start", _serialize_shelly_energy_window_param(start_naive)))
    params.append(("end", _serialize_shelly_energy_window_param(end_naive)))
    params.append(("working_only", working_only))

    url = f"{BASE_URL}/shelly/hourly-energy"
    return _get_shelly_json_params(
        url,
        params,
        log_context=f"hourly-energy ({len(device_ids)} devices)",
    )


def _is_school_hourly_energy_item(item: object) -> bool:
    if not isinstance(item, dict):
        return False
    raw_window_start = item.get("window_start")
    if isinstance(raw_window_start, datetime):
        window_start = raw_window_start
    elif isinstance(raw_window_start, str):
        try:
            window_start = datetime.fromisoformat(raw_window_start.replace("Z", "+00:00"))
        except ValueError:
            return False
    else:
        return False
    return is_school_hour_in_school_timezone(window_start)


def _device_room_key(device: dict) -> str:
    return device.get("room_id") or device.get("room_label") or "unknown-room"


def _three_phase_targets_room(device: dict, room_key: str) -> bool:
    phases = device.get("phases")
    if not isinstance(phases, dict):
        return False
    room_key_norm = str(room_key or "").strip()
    for phase_key in ("a", "b", "c"):
        phase_meta = phases.get(phase_key)
        if not isinstance(phase_meta, dict):
            continue
        if str(phase_meta.get("target_room_id") or "").strip() == room_key_norm:
            return True
    return False


def _three_phase_has_target_metadata(device: dict) -> bool:
    phases = device.get("phases")
    if not isinstance(phases, dict):
        return False
    for phase_key in ("a", "b", "c"):
        phase_meta = phases.get(phase_key)
        if not isinstance(phase_meta, dict):
            continue
        if str(phase_meta.get("target_room_id") or "").strip():
            return True
    return False


def list_school_room_energy_devices(school_id: str, room_key: str) -> list[dict]:
    from monitoring.services.energy_demand.devices import list_energy_devices_for_school

    room_key_norm = str(room_key or "").strip()
    catalog = [
        device
        for device in list_energy_devices_for_school(school_id)
        if device.get("type") in ("plug", "three_phase_meter")
    ]

    if room_key_norm == WHOLE_BUILDING_ROOM_KEY and not _school_has_whole_building_meter(catalog):
        out: list[dict] = []
        for device in catalog:
            device_id = device.get("id")
            if not isinstance(device_id, str) or not device_id:
                continue
            phases = device.get("phases")
            out.append(
                {
                    "id": device_id,
                    "label": _device_display_label(device),
                    "type": device.get("type") or "plug",
                    "phases": phases if isinstance(phases, dict) else None,
                }
            )
        out.sort(key=lambda x: x["id"])
        return out

    out: list[dict] = []
    for device in catalog:
        if device.get("type") not in ("plug", "three_phase_meter"):
            continue
        device_type = device.get("type")
        base_room_matches = _device_room_key(device) == room_key
        if device_type == "three_phase_meter":
            phase_room_matches = _three_phase_targets_room(device, room_key)
            has_phase_targets = _three_phase_has_target_metadata(device)
            if not phase_room_matches and (has_phase_targets or not base_room_matches):
                continue
        elif not base_room_matches:
            continue
        did = device.get("id")
        if not isinstance(did, str) or not did:
            continue
        phases = device.get("phases")
        out.append(
            {
                "id": did,
                "label": _device_display_label(device),
                "type": device.get("type") or "plug",
                "phases": phases if isinstance(phases, dict) else None,
            }
        )
    out.sort(key=lambda x: x["id"])
    return out


def uses_school_wide_all_rooms_aggregate(school_id: str, room_key: str) -> bool:
    from monitoring.services.energy_demand.devices import list_energy_devices_for_school

    room_key_norm = str(room_key or "").strip()
    if room_key_norm != WHOLE_BUILDING_ROOM_KEY:
        return False
    catalog = [
        device
        for device in list_energy_devices_for_school(school_id)
        if device.get("type") in ("plug", "three_phase_meter")
    ]
    return bool(catalog) and not _school_has_whole_building_meter(catalog)


def get_school_room_hourly_energy(
    school_id: str,
    room_key: str,
    *,
    start: datetime | None,
    end: datetime | None,
    working_only: bool,
) -> dict:
    """
    Hourly Wh per room: totals plus per-device slices (plug + Pro 3EM) for stacked charts.
    """
    room_devices = list_school_room_energy_devices(school_id, room_key)
    device_ids = [device["id"] for device in room_devices]
    if uses_school_wide_all_rooms_aggregate(school_id, room_key):
        series_plan = build_school_wide_hourly_series_plan(room_devices)
    else:
        series_plan = build_room_hourly_series_plan(room_devices, room_key)

    if start is None and end is None:
        start_dt, end_dt = default_completed_hour_window_utc(hours=24)
    elif start is None or end is None:
        raise HTTPException(
            status_code=400,
            detail="Provide both start and end, or omit both to use the last 24 completed hours.",
        )
    else:
        start_dt = normalize_api_window_dt(start).replace(second=0, microsecond=0)
        end_dt = normalize_api_window_dt(end).replace(second=0, microsecond=0)
        if end_dt < start_dt:
            raise HTTPException(status_code=400, detail="end must be greater than or equal to start")

    start_str = _serialize_shelly_energy_window_param(start_dt)
    end_str = _serialize_shelly_energy_window_param(end_dt)

    if not device_ids:
        return {
            "room_key": room_key,
            "start": start_str,
            "end": end_str,
            "working_only": working_only,
            **_emissions_factor_contract(),
            "device_ids": [],
            "count": 0,
            "points": [],
        }

    raw = fetch_shelly_hourly_energy(
        device_ids,
        start=start_dt,
        end=end_dt,
        # The upstream flag uses an older, end-exclusive policy. Fetch the
        # complete range so this service can apply the canonical policy.
        working_only=False,
    )
    items = raw.get("items") if isinstance(raw, dict) else None
    if not isinstance(items, list):
        items = []
    if working_only:
        items = [item for item in items if _is_school_hourly_energy_item(item)]

    points = aggregate_shelly_hourly_items_by_window_and_device(
        items,
        series_plan=series_plan,
    )

    up_start = raw.get("start") if isinstance(raw, dict) and isinstance(raw.get("start"), str) else start_str
    up_end = raw.get("end") if isinstance(raw, dict) and isinstance(raw.get("end"), str) else end_str

    return {
        "room_key": room_key,
        "start": up_start,
        "end": up_end,
        "working_only": working_only,
        **_emissions_factor_contract(),
        "device_ids": device_ids,
        "count": len(points),
        "points": points,
    }
