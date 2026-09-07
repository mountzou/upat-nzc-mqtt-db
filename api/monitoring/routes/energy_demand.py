from monitoring.utils.interval_query import history_interval
import logging
from time import perf_counter

from datetime import date
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import AwareDatetime

from monitoring.routes.auth import get_current_user
from monitoring.schemas import EnergyDeviceMetadata
from monitoring.schemas import SchoolEnergyInsightsResponse
from monitoring.services.authentication import AuthUserRecord
from monitoring.services.authorization import enforce_school_access
from monitoring.services.energy_demand.devices import (
    find_school_id_for_energy_device,
    list_energy_devices_for_school,
)
from monitoring.services.energy.school_insights import get_school_energy_insights
from monitoring.services.service_energy_api import fetch_energy_device_history
from monitoring.services.service_energy_api import fetch_energy_device_latest
from monitoring.services.energy_demand.consumption import ConsumptionHistory, bounds, get_consumption

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get('/energy/consumption/history', response_model=ConsumptionHistory)
def consumption_history(
    request: Request, response: Response,
    school_id: str = Query(min_length=1, max_length=100),
    room_id: str | None = Query(default=None, min_length=1, max_length=100),
    start_date: date | None = None, end_date: date | None = None,
    start: AwareDatetime | None = None, end: AwareDatetime | None = None,
    interval: str = '1h', user: AuthUserRecord = Depends(get_current_user),
):
    """Observed consumption in kWh. Dates include both Athens calendar days;
    instant ranges are [start,end), aligned to whole hours. Omit room_id for
    the configured school total. Quality describes persisted hourly coverage.
    """
    enforce_school_access(user, school_id)
    allowed = {'school_id', 'room_id', 'start_date', 'end_date', 'start', 'end', 'interval'}
    if set(request.query_params) - allowed or any(len(request.query_params.getlist(k)) != 1 for k in request.query_params):
        raise HTTPException(422, 'Use school_id, optional room_id, interval and one pair of date or instant bounds')
    if room_id == 'all_rooms':
        raise HTTPException(422, 'Omit room_id for the school total')
    response.headers['Cache-Control'] = 'private, no-store'
    try:
        first, stop = bounds(start_date, end_date, start, end)
        return get_consumption(school_id, room_id, first, stop, interval)
    except (ValueError, OverflowError) as exc:
        raise HTTPException(422, str(exc)) from exc


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
    interval: str = Depends(history_interval),
    limit: int = Query(24, ge=1, le=1000),
    user: AuthUserRecord = Depends(get_current_user),
):
    _enforce_energy_device_access(user, device_id)
    return fetch_energy_device_history(
        device_id,
        aggregate=aggregate,
        interval=interval,
        limit=limit,
        metrics=tuple(metric) if metric else None,
    )
