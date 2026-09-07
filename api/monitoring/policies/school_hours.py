"""Canonical school-hours policy for monitoring and insight buckets."""

from __future__ import annotations

from datetime import datetime

from monitoring.utils.timezone import to_app_timezone

SCHOOL_HOURS_START_HOUR = 8
SCHOOL_HOURS_END_HOUR = 14
SCHOOL_HOURS_BUCKET_LABEL = "Mon–Fri 08:00–14:59 (local bucket time)"


def is_school_hour(*, weekday: int, hour: int) -> bool:
    """Return whether a local weekday/hour bucket belongs to school hours."""
    return 0 <= weekday < 5 and SCHOOL_HOURS_START_HOUR <= hour <= SCHOOL_HOURS_END_HOUR


def is_local_school_hour(value: datetime) -> bool:
    """Evaluate a datetime whose clock fields already represent school-local time."""
    return is_school_hour(weekday=value.weekday(), hour=value.hour)


def is_school_hour_in_school_timezone(value: datetime) -> bool:
    """Convert an instant to the configured school timezone before evaluation.

    Measurement instants must carry an explicit offset. Athens calendar
    rules determine the weekday and school hour.
    """
    local_value = to_app_timezone(value)
    return is_local_school_hour(local_value)
