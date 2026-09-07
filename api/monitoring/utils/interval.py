"""Public bucket contract: fixed elapsed minutes/hours or an Athens calendar day."""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
from zoneinfo import ZoneInfo

ATHENS = ZoneInfo("Europe/Athens")
ORIGIN = datetime(2001, 1, 1, tzinfo=timezone.utc)
# Preserve the largest duration accepted by the old low-level history model.
MAX_MINUTES = 10080 * 24 * 60


@dataclass(frozen=True)
class Interval:
    minutes: int | None

    @property
    def calendar_day(self) -> bool:
        return self.minutes is None

    @property
    def value(self) -> str:
        if self.calendar_day:
            return "day"
        return f"{self.minutes // 60}h" if self.minutes % 60 == 0 else f"{self.minutes}m"

    @property
    def sql_duration(self) -> str:
        if self.calendar_day:
            raise ValueError("A calendar day has no fixed duration")
        number, unit = (self.minutes // 60, "hour") if self.minutes % 60 == 0 else (self.minutes, "minute")
        return f"{number} {unit if number == 1 else unit + 's'}"

    def floor(self, instant: datetime) -> datetime:
        if instant.tzinfo is None or instant.utcoffset() is None:
            raise ValueError("Timestamp must include an explicit timezone offset")
        instant = instant.astimezone(timezone.utc)
        if self.calendar_day:
            return instant.astimezone(ATHENS).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
        stride = timedelta(minutes=self.minutes)
        return ORIGIN + ((instant - ORIGIN) // stride) * stride

    def shift(self, instant: datetime, buckets: int) -> datetime:
        if self.calendar_day:
            return (instant.astimezone(ATHENS) + timedelta(days=buckets)).astimezone(timezone.utc)
        return instant + timedelta(minutes=self.minutes * buckets)


def parse_interval(value: str | Interval) -> Interval:
    if isinstance(value, Interval):
        return value
    if value == "day":
        return Interval(None)
    match = re.fullmatch(r"([1-9][0-9]{0,8})(m|h)", value) if isinstance(value, str) else None
    if not match:
        raise ValueError("interval must be a positive minute/hour duration, e.g. 5m or 1h, or 'day' for an Athens calendar day")
    minutes = int(match[1]) * (60 if match[2] == "h" else 1)
    if minutes > MAX_MINUTES:
        raise ValueError("interval exceeds the supported history duration")
    return Interval(minutes)


RETIRED_BUCKET_PARAMETERS = frozenset({"bucket_unit", "bucket_size", "bucket_minutes"})
