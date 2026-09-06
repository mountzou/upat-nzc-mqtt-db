"""Canonical timezone rules for instants and Athens calendar dates."""

from __future__ import annotations

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from monitoring.config import APP_TIMEZONE_NAME

UTC = timezone.utc
APP_TIMEZONE = ZoneInfo(APP_TIMEZONE_NAME)


def as_utc(value: datetime, *, naive_is_utc: bool = True) -> datetime:
    """Return an aware UTC instant.

    Legacy VPS measurement tables expose UTC values as timezone-naive
    timestamps. Callers may reject that compatibility assumption by passing
    ``naive_is_utc=False``.
    """
    if value.tzinfo is None:
        if not naive_is_utc:
            raise ValueError("Timezone-naive datetime cannot be resolved")
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def to_app_timezone(value: datetime, *, naive_is_utc: bool = True) -> datetime:
    """Convert an instant to DST-aware Europe/Athens time."""
    return as_utc(value, naive_is_utc=naive_is_utc).astimezone(APP_TIMEZONE)


def utc_now() -> datetime:
    """Current aware UTC instant."""
    return datetime.now(UTC)


def utc_now_naive() -> datetime:
    """Current UTC instant for legacy upstream APIs that require naive values."""
    return utc_now().replace(tzinfo=None)


def app_now() -> datetime:
    """Current DST-aware application-local datetime."""
    return datetime.now(APP_TIMEZONE)


def app_today() -> date:
    """Current calendar date in Europe/Athens."""
    return app_now().date()
