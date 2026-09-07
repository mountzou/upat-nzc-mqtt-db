from monitoring.local_data import local_read
from monitoring.read_limits import monitoring_read
from readers.measurements import fetch_device_latest as read_latest
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
from monitoring.utils.api_datetime import get_history_window
from monitoring.utils.api_datetime import normalize_api_window_dt

from monitoring.utils.interval import parse_interval

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
    """Preserve the UTC offset when passing an instant to the local query."""
    return normalize_api_window_dt(dt).replace(second=0).isoformat(timespec="minutes")


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
    with monitoring_read():
        return read_latest("shelly_measurements", device_id, None, limit)


def fetch_energy_device_history(
    device_id: str,
    *,
    aggregate: str,
    interval: str,
    limit: int,
    metrics: tuple[str, ...] | None = None,
):
    url = f"{BASE_URL}/shelly/device/{device_id}/history"
    start, end = get_history_window(
        interval=interval,
        limit=limit,
    )
    params = {
        "start": normalize_api_window_dt(start).isoformat(timespec="minutes"),
        "end": normalize_api_window_dt(end).isoformat(timespec="minutes"),
        "aggregate": aggregate,
        "interval": parse_interval(interval).value,
    }
    if metrics:
        params["metric"] = metrics

    return _get_json(
        url,
        device_id=device_id,
        params=params,
    )


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

    start_utc = normalize_api_window_dt(start).replace(second=0, microsecond=0)
    end_utc = normalize_api_window_dt(end).replace(second=0, microsecond=0)
    if end_utc - start_utc > timedelta(days=90):
        raise HTTPException(400, "Energy history must not exceed 90 days")
    if end_utc < start_utc:
        raise HTTPException(status_code=400, detail="end must be greater than or equal to start")

    params: list[tuple[str, str | bool]] = [("device_id", did) for did in device_ids]
    params.append(("start", _serialize_shelly_energy_window_param(start_utc)))
    params.append(("end", _serialize_shelly_energy_window_param(end_utc)))
    params.append(("working_only", working_only))

    url = f"{BASE_URL}/shelly/hourly-energy"
    return _get_shelly_json_params(
        url,
        params,
        log_context=f"hourly-energy ({len(device_ids)} devices)",
    )
