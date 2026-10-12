from monitoring.local_data import read_device_history
from monitoring.read_limits import monitoring_read
from readers.measurements import fetch_device_latest as read_latest
from datetime import datetime

from fastapi import HTTPException

from schemas import HistoryQueryParams
from monitoring.utils.api_datetime import get_history_window
from monitoring.utils.api_datetime import normalize_api_window_dt
from monitoring.utils.timezone import as_utc

from monitoring.utils.interval import parse_interval

METRICS_OVERVIEW = ("temperature", "relative_humidity", "co2", "voc", "pm25")
METRICS_IAQ_DAILY = ("co2", "pm25")
METRICS_THERMAL_COMFORT = ("temperature", "relative_humidity")


def _parse_event_time(value: str) -> datetime:
    return as_utc(datetime.fromisoformat(value.replace("Z", "+00:00"))).replace(microsecond=0)


def _fetch_batched_metric_history(
    device_id: str,
    query: HistoryQueryParams,
) -> list[dict]:
    try:
        payload = read_device_history("upat", device_id, query)
    except HTTPException as exc:
        if exc.status_code == 404:
            return []
        raise

    try:
        return sorted(
            payload.get("items", []),
            key=lambda item: _parse_event_time(item["event_time"]),
            reverse=True,
        )
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise HTTPException(status_code=502, detail="Malformed upstream device timestamp") from exc


def fetch_device_latest(device_id: str):
    with monitoring_read():
        return read_latest("upat_measurements", device_id, None, 1)


# Fetch historical measurements through the shared local adapter.
def fetch_device_history(
    device_id: str,
    *,
    aggregate: str,
    interval: str,
    limit: int,
    metrics: tuple[str, ...] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
):
    if (start is None) != (end is None):
        raise HTTPException(
            status_code=400,
            detail="start and end must be provided together",
        )
    if start is None or end is None:
        start, end = get_history_window(
            interval=interval,
            limit=limit,
        )
    else:
        start = normalize_api_window_dt(start)
        end = normalize_api_window_dt(end)
        if start >= end:
            raise HTTPException(status_code=400, detail="start must be before end")

    metrics_tuple = metrics if metrics is not None else METRICS_OVERVIEW
    query = HistoryQueryParams(
        start=normalize_api_window_dt(start).isoformat(timespec="minutes"),
        end=normalize_api_window_dt(end).isoformat(timespec="minutes"),
        aggregate=aggregate,
        interval=parse_interval(interval).value,
        limit=limit,
        metric=list(metrics_tuple),
    )
    items = _fetch_batched_metric_history(device_id, query)[:limit]

    if not items:
        return {"device_id": device_id, "count": 0, "items": []}

    return {
        "device_id": device_id,
        "count": len(items),
        "items": items,
    }
