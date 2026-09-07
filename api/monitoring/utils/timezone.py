"""Canonical timezone rules for instants and Athens calendar dates."""

from __future__ import annotations

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from monitoring.config import APP_TIMEZONE_NAME

UTC = timezone.utc
APP_TIMEZONE = ZoneInfo(APP_TIMEZONE_NAME)


def as_utc(value: datetime) -> datetime:
    """Normalize an explicit instant; never guess a missing source timezone."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Timestamp must include an explicit timezone offset")
    return value.astimezone(UTC)


def to_app_timezone(value: datetime) -> datetime:
    """Convert an explicit instant to DST-aware Europe/Athens time."""
    return as_utc(value).astimezone(APP_TIMEZONE)


def utc_now() -> datetime:
    """Current aware UTC instant."""
    return datetime.now(UTC)


def app_now() -> datetime:
    """Current DST-aware application-local datetime."""
    return datetime.now(APP_TIMEZONE)


def app_today() -> date:
    """Current calendar date in Europe/Athens."""
    return app_now().date()
