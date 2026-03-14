from datetime import datetime, time

from fastapi import HTTPException
from pydantic import BaseModel, Field, model_validator


def parse_datetime_bound(value, bound_name):
    # Try parsing a date in the format of YYYY-MM-DD
    try:
        parsed_date = datetime.strptime(value, "%Y-%m-%d").date()
        if bound_name == "start":
            return datetime.combine(parsed_date, time.min)
        return datetime.combine(parsed_date, time.max)
    except ValueError:
        pass

    # Try parsing a date in the format of YYYY-MM-DDTHH:MM
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M")
    except ValueError:
        pass

    # If both parsing attempts fail, raise an error
    raise HTTPException(
        status_code=400,
        detail=(
            f"{bound_name} must use YYYY-MM-DD for a full day or "
            f"YYYY-MM-DDTHH:MM for a specific time"
        ),
    )


class HistoryQueryParams(BaseModel):
    metric: str | None = None
    limit: int = Field(default=100, le=1000)
    aggregate: str | None = None
    bucket: str | None = None
    bucket_unit: str | None = None
    bucket_size: int | None = Field(default=None, ge=1, le=10080)
    start: str | None = None
    end: str | None = None

    @property
    def resolved_metrics(self):
        if not self.metric:
            return None
        return {
            item.strip()
            for item in self.metric.split(",")
            if item.strip()
        }

    @property
    def resolved_start_time(self):
        if self.start is None:
            return None
        return parse_datetime_bound(self.start, "start")

    @property
    def resolved_end_time(self):
        if self.end is None:
            return None
        return parse_datetime_bound(self.end, "end")

    @property
    def resolved_bucket_unit(self):
        if self.bucket_unit:
            return self.bucket_unit
        return self.bucket

    @property
    def resolved_bucket_size(self):
        if self.bucket_size is None:
            return 1
        return self.bucket_size

    @property
    def resolved_bucket_interval(self):
        if self.resolved_bucket_unit is None:
            return None
        unit = self.resolved_bucket_unit
        if self.resolved_bucket_size != 1:
            unit = f"{unit}s"
        return f"{self.resolved_bucket_size} {unit}"

    @model_validator(mode="after")
    def validate_history_query(self):
        start_time = self.resolved_start_time
        end_time = self.resolved_end_time

        # Validate that start_time is not after end_time if both are provided
        if start_time and end_time and start_time > end_time:
            raise ValueError("start must be earlier than or equal to end")

        uses_aggregation = (
            self.aggregate is not None
            or self.bucket is not None
            or self.bucket_unit is not None
            or self.bucket_size is not None
        )
        if not uses_aggregation:
            return self

        if self.aggregate != "avg":
            raise ValueError("aggregate must be 'avg' when provided")

        if self.bucket is not None and self.bucket_unit is not None:
            if self.bucket != self.bucket_unit:
                raise ValueError("bucket and bucket_unit must match when both are provided")

        if self.resolved_bucket_unit not in {"minute", "hour", "day"}:
            raise ValueError(
                "bucket or bucket_unit must be 'minute', 'hour' or 'day' when aggregate is used"
            )

        return self
