import logging
from datetime import datetime

from fastapi import HTTPException
from pydantic import ValidationError

from monitoring.schemas import (
    DeviceHistoryResponse,
    DeviceHistoryBucketItem,
    DeviceLatestOverviewResponse,
)
from monitoring.services.service_device_api import fetch_device_history
from monitoring.services.service_device_api import fetch_device_latest
from monitoring.utils.timezone import to_app_timezone

logger = logging.getLogger(__name__)


def get_device_latest_overview(device_id: str) -> DeviceLatestOverviewResponse:
    payload = fetch_device_latest(device_id)

    try:
        latest_history = DeviceHistoryResponse.model_validate(payload)
    except ValidationError as exc:
        logger.exception("Invalid upstream metric payload for device '%s'", device_id)
        raise HTTPException(status_code=502, detail="Malformed upstream device payload") from exc

    if latest_history.device_id != device_id:
        logger.warning(
            "Unexpected device id in latest payload for '%s': %s",
            device_id,
            latest_history.device_id,
        )
        raise HTTPException(status_code=502, detail="Malformed upstream device payload")

    if not latest_history.items:
        raise HTTPException(status_code=404, detail=f"No latest data found for device '{device_id}'")

    row_device_ids = {item.device_id for item in latest_history.items}
    if row_device_ids != {device_id}:
        logger.warning(
            "Mixed or unexpected device ids in upstream payload for '%s': %s",
            device_id,
            sorted(row_device_ids),
        )
        raise HTTPException(status_code=502, detail="Malformed upstream device payload")

    latest_item = max(latest_history.items, key=lambda item: item.event_time)

    return DeviceLatestOverviewResponse(
        device_id=device_id,
        latest_event_time=to_app_timezone(latest_item.event_time),
        readings=latest_item.measurements,
    )


def _validate_device_history(history: DeviceHistoryResponse, device_id: str) -> None:
    if history.device_id != device_id:
        logger.warning(
            "Unexpected device id in history payload for '%s': %s",
            device_id,
            history.device_id,
        )
        raise HTTPException(status_code=502, detail="Malformed upstream device payload")

    if not history.items:
        raise HTTPException(status_code=404, detail=f"No history found for device '{device_id}'")

    row_device_ids = {item.device_id for item in history.items}
    if row_device_ids != {device_id}:
        logger.warning(
            "Mixed or unexpected device ids in history payload for '%s': %s",
            device_id,
            sorted(row_device_ids),
        )
        raise HTTPException(status_code=502, detail="Malformed upstream device payload")


def get_device_rolling_twenty_four_hour_hourly_series(device_id: str) -> DeviceHistoryResponse:
    """Completed clock hours from canonical SQL in the stable home envelope."""
    from monitoring.services.environment.history import resolve_history_plan, get_environmental_history
    plan = resolve_history_plan(window="24h", interval="1h", alignment="clock", limit=24)
    return DeviceHistoryResponse.model_validate(get_environmental_history(device_id, plan).model_dump())


def get_device_history(
    device_id: str,
    *,
    aggregate: str,
    interval: str,
    limit: int,
    metrics: tuple[str, ...] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> DeviceHistoryResponse:
    payload = fetch_device_history(
        device_id,
        aggregate=aggregate,
        interval=interval,
        limit=limit,
        metrics=metrics,
        start=start,
        end=end,
    )

    try:
        history = DeviceHistoryResponse.model_validate(payload)
    except ValidationError as exc:
        logger.exception("Invalid upstream history payload for device '%s'", device_id)
        raise HTTPException(status_code=502, detail="Malformed upstream device payload") from exc

    _validate_device_history(history, device_id)

    adjusted_items = [
        DeviceHistoryBucketItem(
            device_id=item.device_id,
            event_time=to_app_timezone(item.event_time),
            measurements=item.measurements,
        )
        for item in history.items
    ]
    return DeviceHistoryResponse(
        device_id=history.device_id,
        count=history.count,
        items=adjusted_items,
    )
