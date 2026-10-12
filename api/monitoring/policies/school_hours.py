"""Canonical school-hours policy for monitoring and insight buckets."""

from __future__ import annotations

from datetime import datetime

SCHOOL_HOURS_START_HOUR = 8
SCHOOL_HOURS_END_HOUR = 14


def is_school_hour(*, weekday: int, hour: int) -> bool:
    """Return whether a local weekday/hour bucket belongs to school hours."""
    return 0 <= weekday < 5 and SCHOOL_HOURS_START_HOUR <= hour <= SCHOOL_HOURS_END_HOUR


def is_local_school_hour(value: datetime) -> bool:
    """Evaluate a datetime whose clock fields already represent school-local time."""
    return is_school_hour(weekday=value.weekday(), hour=value.hour)
