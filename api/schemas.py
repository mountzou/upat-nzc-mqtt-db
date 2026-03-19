from datetime import datetime, time
from fastapi import HTTPException
from pydantic import BaseModel, Field, model_validator

# Parses a datetime bound from a string value
def parse_datetime_bound(date, bound_name):
    # Try parsing a date in the format of YYYY-MM-DD
    try:
        parsed_date = datetime.strptime(date, "%Y-%m-%d").date()
        # If it's a start bound, return YYYY-MM-DDT00:00:00
        if bound_name == "start":
            return datetime.combine(parsed_date, time.min)
        # If it's an end bound, return YYYY-MM-DDT23:59:59
        return datetime.combine(parsed_date, time.max)
    except ValueError:
        pass

    # Try parsing a date in the format of YYYY-MM-DDTHH:MM
    try:
        return datetime.strptime(date, "%Y-%m-%dT%H:%M")
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


# Normalizes a list of metric names by stripping whitespace, removing empty entries, and sorting them
def normalize_metrics(metrics):
    if not metrics:
        return None

    return sorted(
        {
            item.strip()
            for item in metrics
            if item and item.strip()
        }
    ) or None


# Pydantic model for validating and processing historical query parameters for the device history endpoint
class HistoryQueryParams(BaseModel):
    metric: list[str] | None = None
    start: str | None = None
    end: str | None = None
    aggregate: str | None = None
    bucket_unit: str | None = None
    bucket_size: int | None = Field(default=None, ge=1, le=10080)
    limit: int = Field(default=100, le=1000)

    @property   # Return the resolved list of normalized metric names or None if no metrics were provided
    def resolved_metrics(self):
        return normalize_metrics(self.metric)

    @property   # Return the resolved start time as a datetime object or None if not provided
    def resolved_start_time(self):
        if self.start is None:
            return None
        return parse_datetime_bound(self.start, "start")

    @property   # Return the resolved end time as a datetime object or None if not provided
    def resolved_end_time(self):
        if self.end is None:
            return None
        return parse_datetime_bound(self.end, "end")

    @property   # Return the resolved bucket unit, such as "minute", "hour" or "day"
    def resolved_bucket_unit(self):
        return self.bucket_unit

    @property   # Return the resolved bucket size, such as 1, 5 or 60, with a default of 1
    def resolved_bucket_size(self):
        if self.bucket_size is None:
            return 1
        return self.bucket_size

    @property   # Return the resolved bucket interval as a string like "5 minutes" or "1 hour"
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
        end_time   = self.resolved_end_time

        # Validate that both start_time and end_time are provided or both are omitted
        if (self.start is None) != (self.end is None):
            raise ValueError("start and end must either both be provided or both be omitted")

        # Validate that start_time is not after end_time if both are provided
        if start_time and end_time and start_time > end_time:
            raise ValueError("start must be earlier than or equal to end")

        # If the user did not provide any aggregation parameters, return the original object without further validation
        uses_aggregation = (
            self.aggregate is not None
            or self.bucket_unit is not None
            or self.bucket_size is not None
        )
        if not uses_aggregation:
            return self

        # Validate that if the user provided an aggregate parameter, it must be "avg"
        if self.aggregate != "avg":
            raise ValueError("aggregate must be 'avg' when provided")

        # Validate that if the user provided a bucket_unit parameter, it must be "minute", "hour" or "day"
        if self.resolved_bucket_unit not in {"minute", "hour", "day"}:
            raise ValueError("bucket_unit must be 'minute', 'hour' or 'day' when aggregate is used")

        return self