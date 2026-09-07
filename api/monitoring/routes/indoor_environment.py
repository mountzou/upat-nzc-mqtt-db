import logging
from monitoring.services.environment.history.contract import HistoryPlan, history_plan
from monitoring.services.environment.history.service import EnvironmentalHistoryResponse, get_environmental_history
from fastapi.responses import StreamingResponse
from time import perf_counter
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Response

from monitoring.config import APP_TIMEZONE_NAME
from monitoring.routes.auth import get_current_user
from monitoring.policies.iaq import build_iaq_policy_payload
from monitoring.schemas import (
    HomeEnvironmentRequest,
    HomeEnvironmentResponse,
    DeviceLatestOverviewResponse,
    DiscomfortIndexResponse,
    HumidexResponse,
    IAQPolicyResponse,
    IndoorEnvironmentAIQuestionRequest,
    SchoolEnvironmentalInsightsResponse,
    SchoolDeviceMetadata,
)
from monitoring.services.authentication import AuthUserRecord
from monitoring.services.authorization import enforce_school_access
from monitoring.services.environment.devices import (
    find_school_id_for_device,
    list_devices_for_school,
)
from monitoring.services.environment.school_insights import (
    get_school_environmental_insights,
)
from monitoring.services.environment.thermal_comfort import (
    calc_discomfort_index,
    calc_hum_index,
)
from monitoring.services.service_overview import (
    get_device_latest_overview,
)
from monitoring.utils.timezone import app_today

from monitoring.services.environment.home_overview import get_home_overview, iter_home_overview, resolve_home_rooms

router = APIRouter()
logger = logging.getLogger("uvicorn.error.environment_history")

TdbQuery = Annotated[float, Query(..., ge=15, le=35, description="Air temperature [°C]")]
RhQuery = Annotated[float, Query(..., ge=0, le=100, description="Relative humidity [%]")]


def _enforce_environment_device_access(user: AuthUserRecord, device_id: str) -> str:
    school_id = find_school_id_for_device(device_id)
    if school_id is None:
        raise HTTPException(status_code=404, detail="Environmental device not found.")
    enforce_school_access(user, school_id)
    return school_id


@router.get(
    "/indoor_environment/policy",
    response_model=IAQPolicyResponse,
)
def get_indoor_environment_policy(response: Response):
    response.headers["Cache-Control"] = "public, max-age=3600"
    return build_iaq_policy_payload()


@router.get(
    "/indoor_environment/devices/{device_id}/latest",
    response_model=DeviceLatestOverviewResponse,
)
def get_overview_device_latest(
    device_id: str,
    user: AuthUserRecord = Depends(get_current_user),
):
    _enforce_environment_device_access(user, device_id)
    return get_device_latest_overview(device_id)


@router.get(
    "/indoor_environment/schools/{school_id}/devices",
    response_model=list[SchoolDeviceMetadata],
)
def get_overview_school_devices(
    school_id: str,
    user: AuthUserRecord = Depends(get_current_user),
):
    enforce_school_access(user, school_id)
    return list_devices_for_school(school_id)


@router.post(
    "/indoor_environment/schools/{school_id}/home",
    response_model=HomeEnvironmentResponse,
)
def get_school_home(school_id: str, payload: HomeEnvironmentRequest, response: Response,
                    user: AuthUserRecord = Depends(get_current_user)):
    enforce_school_access(user, school_id)
    # Resolve rooms within the authorized catalog before any cached/upstream read.
    devices = list_devices_for_school(school_id)
    if payload.stream:
        # Validate before sending 200/headers; errors remain normal HTTP errors.
        rooms = resolve_home_rooms(list(dict.fromkeys(payload.room_ids)), devices)
        def updates():
            for update in iter_home_overview(school_id, rooms, payload.refresh):
                yield HomeEnvironmentResponse.model_validate(update).model_dump_json() + "\n"
        return StreamingResponse(updates(), media_type="application/x-ndjson", headers={
            "Cache-Control": "private, no-store", "X-Accel-Buffering": "no"
        })
    started = perf_counter()
    result = get_home_overview(school_id, list(dict.fromkeys(payload.room_ids)), devices, payload.refresh)
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["Server-Timing"] = f"home;dur={(perf_counter() - started) * 1000:.1f}"
    return result


@router.get(
    "/indoor_environment/schools/{school_id}/insights",
    response_model=SchoolEnvironmentalInsightsResponse,
)
def get_school_insights(
    school_id: str,
    response: Response,
    user: AuthUserRecord = Depends(get_current_user),
):
    enforce_school_access(user, school_id)
    started = perf_counter()
    payload = get_school_environmental_insights(school_id)
    elapsed_ms = (perf_counter() - started) * 1000
    response.headers["Server-Timing"] = f"school-insights;dur={elapsed_ms:.1f}"
    return payload




@router.get(
    "/indoor_environment/devices/{device_id}/history",
    response_model=EnvironmentalHistoryResponse,
)
def get_overview_device_history(
    device_id: str,
    response: Response,
    plan: HistoryPlan = Depends(history_plan),
    user: AuthUserRecord = Depends(get_current_user),
):
    _enforce_environment_device_access(user, device_id)
    logger.info('environment_history contract=canonical preset=history')
    response.headers['X-Environmental-History-Contract'] = 'canonical'
    return get_environmental_history(device_id, plan)


@router.get(
    "/thermal-comfort/discomfort-index",
    response_model=DiscomfortIndexResponse,
    tags=["thermal-comfort"],
)
def get_discomfort_index(tdb: TdbQuery, rh: RhQuery):
    di_val, discomfort_condition = calc_discomfort_index(tdb=tdb, rh=rh)
    return DiscomfortIndexResponse(di=di_val, discomfort_condition=discomfort_condition)


@router.get(
    "/thermal-comfort/humidex",
    response_model=HumidexResponse,
    tags=["thermal-comfort"],
)
def get_humidex(tdb: TdbQuery, rh: RhQuery):
    humidex_val, discomfort = calc_hum_index(tdb=tdb, rh=rh)
    return HumidexResponse(humidex=humidex_val, discomfort=discomfort)
