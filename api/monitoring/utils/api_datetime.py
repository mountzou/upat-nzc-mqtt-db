"""UTC datetimes and query params for the device / Shelly HTTP API."""

from datetime import datetime, timedelta

from fastapi import HTTPException

from monitoring.utils.timezone import as_utc, utc_now_naive


def normalize_api_window_dt(dt: datetime) -> datetime:
    if dt.tzinfo is not None:
        dt = as_utc(dt).replace(tzinfo=None)
    return dt.replace(microsecond=0)


def _floor_to_bucket_boundary(dt: datetime, *, bucket_unit: str, bucket_size: int) -> datetime:
    """Floor UTC `dt` to the start of the current (possibly incomplete) bucket."""
    dt = dt.replace(microsecond=0)
    if bucket_unit == "minute":
        minute = (dt.minute // bucket_size) * bucket_size
        return dt.replace(minute=minute, second=0)
    if bucket_unit == "hour":
        hour = (dt.hour // bucket_size) * bucket_size
        return dt.replace(hour=hour, minute=0, second=0)
    if bucket_unit == "day":
        return dt.replace(hour=0, minute=0, second=0)
    raise HTTPException(status_code=400, detail=f"Unsupported bucket_unit '{bucket_unit}'")


def get_history_window(*, bucket_unit: str, bucket_size: int, limit: int) -> tuple[datetime, datetime]:
    """
    Return [start, end) for upstream history queries.

    Hour/day buckets from the IoT API only include complete buckets and require
    start/end aligned to bucket boundaries (non-aligned windows return empty).
    """
    now = utc_now_naive().replace(second=0, microsecond=0)
    if bucket_unit in {"hour", "day"}:
        end = _floor_to_bucket_boundary(now, bucket_unit=bucket_unit, bucket_size=bucket_size)
        # Fetch one extra bucket upstream; aggregated hour/day buckets often lag by one interval.
        span = bucket_size * (limit + 1)
        unit_to_delta = {
            "hour": timedelta(hours=span),
            "day": timedelta(days=span),
        }
        return end - unit_to_delta[bucket_unit], end

    unit_to_delta = {
        "minute": timedelta(minutes=bucket_size * limit),
    }
    if bucket_unit not in unit_to_delta:
        raise HTTPException(status_code=400, detail=f"Unsupported bucket_unit '{bucket_unit}'")
    return now - unit_to_delta[bucket_unit], now
