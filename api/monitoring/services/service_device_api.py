from monitoring.local_data import local_read
import logging
from collections import defaultdict
from datetime import datetime, timezone
from math import ceil

from fastapi import HTTPException

from monitoring.config import DEVICE_API_BASE_URL
from monitoring.utils.api_datetime import get_history_window
from monitoring.utils.api_datetime import normalize_api_window_dt

logger = logging.getLogger(__name__)

METRICS_OVERVIEW = ("temperature", "relative_humidity", "co2", "voc", "pm25")
METRICS_IAQ_DAILY = ("co2", "pm25")
METRICS_THERMAL_COMFORT = ("temperature", "relative_humidity")
UPSTREAM_HISTORY_MAX_LIMIT = 1000


def _get_json(url: str, *, device_id: str, params: dict | None = None):
    return local_read(url, params)


def _parse_event_time(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.replace(microsecond=0)


def _floor_to_coarse_bucket(dt: datetime, *, bucket_unit: str, bucket_size: int) -> datetime:
    dt = dt.replace(microsecond=0)
    if bucket_unit == "hour":
        hour = (dt.hour // bucket_size) * bucket_size
        return dt.replace(hour=hour, minute=0, second=0)
    if bucket_unit == "day":
        return dt.replace(hour=0, minute=0, second=0)
    raise ValueError(f"Unsupported coarse bucket_unit '{bucket_unit}'")


def _average_raw_measurements(items: list[dict]) -> dict:
    sums: dict[str, float] = {}
    counts: dict[str, int] = {}
    units: dict[str, str | None] = {}
    for item in items:
        measurements = item.get("measurements") or {}
        if not isinstance(measurements, dict):
            continue
        for key, reading in measurements.items():
            if not isinstance(reading, dict):
                continue
            value = reading.get("value")
            if value is None:
                continue
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                continue
            sums[key] = sums.get(key, 0.0) + numeric
            counts[key] = counts.get(key, 0) + 1
            units.setdefault(key, reading.get("unit"))
    return {
        key: {"value": sums[key] / counts[key], "unit": units.get(key)}
        for key in sums
        if counts[key] > 0
    }


def _fetch_batched_metric_history(
    url: str,
    device_id: str,
    base_params: dict,
    metrics: tuple[str, ...],
) -> list[dict]:
    try:
        payload = _get_json(
            url,
            device_id=device_id,
            params={**base_params, "metric": metrics},
        )
    except HTTPException as exc:
        if exc.status_code == 404:
            return []
        raise

    return sorted(
        payload.get("items", []),
        key=lambda item: item.get("event_time", ""),
        reverse=True,
    )


def _aggregate_minute_items_to_coarse_buckets(
    minute_items: list[dict],
    *,
    device_id: str,
    bucket_unit: str,
    bucket_size: int,
    limit: int,
    window_start: datetime,
    window_end: datetime,
) -> list[dict]:
    """Build hour/day buckets by averaging minute-level readings inside each window."""
    groups: dict[datetime, list[dict]] = defaultdict(list)
    for item in minute_items:
        event_time = item.get("event_time")
        if not event_time:
            continue
        try:
            parsed_time = _parse_event_time(event_time)
        except ValueError:
            logger.warning("Skipping history item with invalid event_time '%s'", event_time)
            continue
        if parsed_time < window_start or parsed_time >= window_end:
            continue
        bucket_start = _floor_to_coarse_bucket(
            parsed_time,
            bucket_unit=bucket_unit,
            bucket_size=bucket_size,
        )
        groups[bucket_start].append(item)

    aggregated: list[dict] = []
    for bucket_start in sorted(groups.keys(), reverse=True):
        measurements = _average_raw_measurements(groups[bucket_start])
        if not measurements:
            continue
        aggregated.append(
            {
                "device_id": device_id,
                "event_time": bucket_start.isoformat(timespec="seconds"),
                "measurements": measurements,
            }
        )

    return aggregated[:limit]


# Fetch latest measurements for a given device ID from the `upat-nzc-mqtt-db` API
def fetch_device_latest(device_id: str):
    url = f"{DEVICE_API_BASE_URL.rstrip('/')}/upat/device/{device_id}/latest"
    return _get_json(url, device_id=device_id, params={"limit": 1})


# Fetch historical measurements for a given device ID from the `upat-nzc-mqtt-db` API
def fetch_device_history(
    device_id: str,
    *,
    aggregate: str,
    bucket_unit: str,
    bucket_size: int,
    limit: int,
    metrics: tuple[str, ...] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
):
    url = f"{DEVICE_API_BASE_URL.rstrip('/')}/upat/device/{device_id}/history"
    if (start is None) != (end is None):
        raise HTTPException(
            status_code=400,
            detail="start and end must be provided together",
        )
    if start is None or end is None:
        start, end = get_history_window(
            bucket_unit=bucket_unit,
            bucket_size=bucket_size,
            limit=limit,
        )
    else:
        start = normalize_api_window_dt(start)
        end = normalize_api_window_dt(end)
        if start >= end:
            raise HTTPException(status_code=400, detail="start must be before end")

    base_params = {
        "start": normalize_api_window_dt(start).isoformat(timespec="minutes"),
        "end": normalize_api_window_dt(end).isoformat(timespec="minutes"),
        "aggregate": aggregate,
        "bucket_unit": bucket_unit,
        "bucket_size": bucket_size,
        "limit": limit,
    }

    metrics_tuple = metrics if metrics is not None else METRICS_OVERVIEW
    items = _fetch_batched_metric_history(
        url,
        device_id,
        base_params,
        metrics_tuple,
    )[:limit]

    if not items:
        return {"device_id": device_id, "count": 0, "items": []}

    return {
        "device_id": device_id,
        "count": len(items),
        "items": items,
    }
