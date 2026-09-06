import logging
from datetime import datetime
from time import perf_counter

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import ValidationError

from monitoring.policies.school_hours import SCHOOL_HOURS_BUCKET_LABEL
from monitoring.routes.auth import get_current_user
from monitoring.schemas import EnergyDeviceMetadata
from monitoring.schemas import RoomHourlyEnergyResponse
from monitoring.schemas import SchoolEnergyInsightsResponse
from monitoring.schemas import ShellyPro3emEnergyEstimateResponse
from monitoring.services.authentication import AuthUserRecord
from monitoring.services.authorization import enforce_school_access
from monitoring.services.energy_demand.devices import (
    find_school_id_for_energy_device,
    list_energy_devices_for_school,
)
from monitoring.services.energy.school_insights import get_school_energy_insights
from monitoring.services.service_energy_api import fetch_energy_device_history
from monitoring.services.service_energy_api import fetch_energy_device_latest
from monitoring.services.service_energy_api import fetch_shelly_pro3em_phase_energy_estimate
from monitoring.services.service_energy_api import get_school_room_hourly_energy

logger = logging.getLogger(__name__)

router = APIRouter()


def _enforce_energy_device_access(user: AuthUserRecord, device_id: str) -> str:
    school_id = find_school_id_for_energy_device(device_id)
    if school_id is None:
        raise HTTPException(status_code=404, detail="Energy device not found.")
    enforce_school_access(user, school_id)
    return school_id


@router.get(
    "/energy/schools/{school_id}/devices",
    response_model=list[EnergyDeviceMetadata],
)
def get_energy_school_devices(
    school_id: str,
    user: AuthUserRecord = Depends(get_current_user),
):
    enforce_school_access(user, school_id)
    return list_energy_devices_for_school(school_id)


@router.get(
    "/energy/schools/{school_id}/rooms/hourly-energy",
    response_model=RoomHourlyEnergyResponse,
)
def get_school_room_hourly_energy_endpoint(
    school_id: str,
    room_key: str = Query(..., min_length=1, description="Room key (matches device room_id / room_label)."),
    start: datetime | None = Query(None, description="Range start (UTC). Omit with end to use API default."),
    end: datetime | None = Query(None, description="Range end (UTC). Hourly buckets must lie fully within [start, end]."),
    working_only: bool = Query(
        False,
        description=f"Restrict to {SCHOOL_HOURS_BUCKET_LABEL} when true.",
    ),
    user: AuthUserRecord = Depends(get_current_user),
):
    enforce_school_access(user, school_id)
    return get_school_room_hourly_energy(
        school_id,
        room_key,
        start=start,
        end=end,
        working_only=working_only,
    )


@router.get(
    "/energy/schools/{school_id}/insights",
    response_model=SchoolEnergyInsightsResponse,
)
def get_school_energy_insights_endpoint(
    school_id: str,
    response: Response,
    user: AuthUserRecord = Depends(get_current_user),
):
    enforce_school_access(user, school_id)
    started = perf_counter()
    payload = get_school_energy_insights(school_id)
    elapsed_ms = (perf_counter() - started) * 1000
    response.headers["Server-Timing"] = f"school-energy-insights;dur={elapsed_ms:.1f}"
    return payload


@router.get("/energy/devices/{device_id}/latest")
def get_energy_device_latest(
    device_id: str,
    limit: int = Query(4, ge=1, le=1000),
    user: AuthUserRecord = Depends(get_current_user),
):
    _enforce_energy_device_access(user, device_id)
    return fetch_energy_device_latest(device_id, limit=limit)


@router.get("/energy/devices/{device_id}/history")
def get_energy_device_history(
    device_id: str,
    metric: list[str] | None = Query(
        None,
        description="Optional metric filter. Repeat the parameter for multiple metrics.",
    ),
    aggregate: str = Query("avg"),
    bucket_unit: str = Query("hour"),
    bucket_size: int = Query(1, ge=1, le=1000),
    limit: int = Query(24, ge=1, le=1000),
    user: AuthUserRecord = Depends(get_current_user),
):
    _enforce_energy_device_access(user, device_id)
    return fetch_energy_device_history(
        device_id,
        aggregate=aggregate,
        bucket_unit=bucket_unit,
        bucket_size=bucket_size,
        limit=limit,
        metrics=tuple(metric) if metric else None,
    )


@router.get(
    "/energy/devices/{device_id}/pro3em-energy",
    response_model=ShellyPro3emEnergyEstimateResponse,
)
def get_shelly_pro3em_energy_estimate(
    device_id: str,
    start: datetime = Query(..., description="Inclusive window start (ISO 8601)."),
    end: datetime = Query(..., description="Inclusive window end (ISO 8601)."),
    bucket_minutes: int = Query(
        30,
        ge=1,
        le=10080,
        description="Aggregation bucket size in minutes (smaller → more accurate, slower upstream query).",
    ),
    user: AuthUserRecord = Depends(get_current_user),
):
    """
    Shelly Pro 3EM only (upstream optimized endpoint): estimated phase and total energy (Wh)
    over [start, end] via bucketed average active power. Does not replace /history; use for
    benchmarking or aggregations over long windows.
    """
    _enforce_energy_device_access(user, device_id)
    raw = fetch_shelly_pro3em_phase_energy_estimate(
        device_id,
        start=start,
        end=end,
        bucket_minutes=bucket_minutes,
    )
    try:
        return ShellyPro3emEnergyEstimateResponse.model_validate(raw)
    except ValidationError as exc:
        logger.warning("Pro 3EM /energy response failed validation for '%s': %s", device_id, exc)
        raise HTTPException(
            status_code=502,
            detail="Shelly /energy response did not match expected shape",
        ) from exc
