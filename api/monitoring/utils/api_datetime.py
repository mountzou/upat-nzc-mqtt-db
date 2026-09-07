"""Explicit instants and interval-aligned history windows."""
from datetime import datetime, timedelta
from fastapi import HTTPException
from monitoring.utils.timezone import as_utc, utc_now
from monitoring.utils.interval import parse_interval


def normalize_api_window_dt(dt: datetime) -> datetime:
    return as_utc(dt).replace(microsecond=0)


def _floor_to_bucket_boundary(dt: datetime, *, interval: str) -> datetime:
    return parse_interval(interval).floor(dt)


def get_history_window(*, interval: str, limit: int) -> tuple[datetime, datetime]:
    """Return an aware [start, end) window; day boundaries follow Athens DST."""
    spec = parse_interval(interval)
    now = utc_now().replace(second=0, microsecond=0)
    if spec.calendar_day or spec.minutes >= 60:
        end = spec.floor(now)
        # Preserve the extra complete bucket used to cover rollup lag.
        try:
            return spec.shift(end, -(limit + 1)), end
        except OverflowError as exc:
            raise HTTPException(422, "interval and limit exceed the supported timestamp range") from exc
    return now - timedelta(minutes=spec.minutes * limit), now
