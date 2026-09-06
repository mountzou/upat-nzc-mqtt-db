import logging
from collections import defaultdict
from datetime import datetime

from fastapi import HTTPException
from pydantic import ValidationError

from monitoring.schemas import (
    DeviceHistoryResponse,
    DeviceHistoryBucketItem,
    DeviceLatestOverviewResponse,
    OverviewReading,
)
from monitoring.services.service_device_api import METRICS_OVERVIEW
from monitoring.services.service_device_api import fetch_device_history
from monitoring.services.service_device_api import fetch_device_latest
from monitoring.utils.api_datetime import get_history_window
from monitoring.utils.timezone import to_app_timezone

logger = logging.getLogger(__name__)

# Sub-hour buckets for rolling means (upstream hourly aggregates lag behind real time).
ROLLING_ONE_HOUR_BUCKET_SIZE_MINUTES = 5
ROLLING_ONE_HOUR_BUCKET_LIMIT = 12
ROLLING_ONE_DAY_BUCKET_SIZE_MINUTES = 15
ROLLING_ONE_DAY_BUCKET_LIMIT = 24 * 4
ROLLING_ONE_DAY_HOURLY_BUCKETS = 24


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


def _average_history_measurements(items: list[DeviceHistoryBucketItem]) -> dict[str, OverviewReading]:
    sums: dict[str, float] = {}
    counts: dict[str, int] = {}
    units: dict[str, str | None] = {}
    for item in items:
        for key, reading in item.measurements.items():
            sums[key] = sums.get(key, 0.0) + float(reading.value)
            counts[key] = counts.get(key, 0) + 1
            units.setdefault(key, reading.unit)
    return {
        key: OverviewReading(value=sums[key] / counts[key], unit=units[key])
        for key in sums
        if counts[key] > 0
    }


def get_device_rolling_minute_average(
    device_id: str,
    *,
    bucket_size_minutes: int,
    bucket_limit: int,
    metrics: tuple[str, ...],
) -> DeviceHistoryResponse:
    """Mean of fixed-size minute buckets over a rolling window ending at now."""
    payload = fetch_device_history(
        device_id,
        aggregate="avg",
        bucket_unit="minute",
        bucket_size=bucket_size_minutes,
        limit=bucket_limit,
        metrics=metrics,
    )

    try:
        history = DeviceHistoryResponse.model_validate(payload)
    except ValidationError as exc:
        logger.exception("Invalid upstream history payload for device '%s'", device_id)
        raise HTTPException(status_code=502, detail="Malformed upstream device payload") from exc

    _validate_device_history(history, device_id)

    _, window_end = get_history_window(
        bucket_unit="minute",
        bucket_size=bucket_size_minutes,
        limit=bucket_limit,
    )
    return DeviceHistoryResponse(
        device_id=device_id,
        count=1,
        items=[
            DeviceHistoryBucketItem(
                device_id=device_id,
                event_time=to_app_timezone(window_end),
                measurements=_average_history_measurements(history.items),
            )
        ],
    )


def get_device_rolling_one_hour_average(device_id: str) -> DeviceHistoryResponse:
    """Mean of the last 12 five-minute buckets (rolling hour)."""
    return get_device_rolling_minute_average(
        device_id,
        bucket_size_minutes=ROLLING_ONE_HOUR_BUCKET_SIZE_MINUTES,
        bucket_limit=ROLLING_ONE_HOUR_BUCKET_LIMIT,
        metrics=METRICS_OVERVIEW,
    )


def _floor_to_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def _group_history_items_by_hour(
    items: list[DeviceHistoryBucketItem],
) -> dict[datetime, list[DeviceHistoryBucketItem]]:
    groups: dict[datetime, list[DeviceHistoryBucketItem]] = defaultdict(list)
    for item in items:
        groups[_floor_to_hour(item.event_time)].append(item)
    return groups


def get_device_rolling_twenty_four_hour_hourly_series(device_id: str) -> DeviceHistoryResponse:
    """Hourly means for the last 24 clock hours, derived from fifteen-minute buckets."""
    payload = fetch_device_history(
        device_id,
        aggregate="avg",
        bucket_unit="minute",
        bucket_size=ROLLING_ONE_DAY_BUCKET_SIZE_MINUTES,
        limit=ROLLING_ONE_DAY_BUCKET_LIMIT,
        metrics=METRICS_OVERVIEW,
    )

    try:
        history = DeviceHistoryResponse.model_validate(payload)
    except ValidationError as exc:
        logger.exception("Invalid upstream history payload for device '%s'", device_id)
        raise HTTPException(status_code=502, detail="Malformed upstream device payload") from exc

    if history.device_id != device_id:
        logger.warning(
            "Unexpected device id in history payload for '%s': %s",
            device_id,
            history.device_id,
        )
        raise HTTPException(status_code=502, detail="Malformed upstream device payload")

    if not history.items:
        raise HTTPException(status_code=404, detail=f"No history found for device '{device_id}'")

    window_start, window_end = get_history_window(
        bucket_unit="minute",
        bucket_size=ROLLING_ONE_DAY_BUCKET_SIZE_MINUTES,
        limit=ROLLING_ONE_DAY_BUCKET_LIMIT,
    )
    window_items = [
        item
        for item in history.items
        if window_start <= item.event_time <= window_end
    ]
    if not window_items:
        raise HTTPException(status_code=404, detail=f"No history found for device '{device_id}'")

    grouped = _group_history_items_by_hour(window_items)
    hour_keys = sorted(grouped.keys())[-ROLLING_ONE_DAY_HOURLY_BUCKETS:]
    hourly_items = [
        DeviceHistoryBucketItem(
            device_id=device_id,
            event_time=to_app_timezone(hour_start),
            measurements=_average_history_measurements(grouped[hour_start]),
        )
        for hour_start in hour_keys
    ]

    return DeviceHistoryResponse(
        device_id=device_id,
        count=len(hourly_items),
        items=hourly_items,
    )


def get_device_history(
    device_id: str,
    *,
    aggregate: str,
    bucket_unit: str,
    bucket_size: int,
    limit: int,
    metrics: tuple[str, ...] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> DeviceHistoryResponse:
    payload = fetch_device_history(
        device_id,
        aggregate=aggregate,
        bucket_unit=bucket_unit,
        bucket_size=bucket_size,
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
