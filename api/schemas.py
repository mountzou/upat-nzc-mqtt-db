from datetime import datetime, time, timezone
from fastapi import HTTPException, Query, Request
from pydantic import BaseModel, Field, ValidationError, model_validator
from monitoring.utils.interval import parse_interval, RETIRED_BUCKET_PARAMETERS

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


def parse_telemetry_bound(value, bound_name):
    """Parse an absolute instant; old offset-free request bounds remain UTC.

    This is the single compatibility boundary for existing callers. Weather
    forecast wall-clock timestamps continue to use parse_datetime_bound.
    """
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        parsed = None
    if parsed is not None and parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc)
    return parse_datetime_bound(value, bound_name).replace(tzinfo=timezone.utc)


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
    interval: str | None = None
    limit: int = Field(default=100, le=1000)

    @property   # Return the resolved list of normalized metric names or None if no metrics were provided
    def resolved_metrics(self):
        return normalize_metrics(self.metric)

    @property   # Return the resolved start time as a datetime object or None if not provided
    def resolved_start_time(self):
        if self.start is None:
            return None
        return parse_telemetry_bound(self.start, "start")

    @property   # Return the resolved end time as a datetime object or None if not provided
    def resolved_end_time(self):
        if self.end is None:
            return None
        return parse_telemetry_bound(self.end, "end")

    @property
    def resolved_interval(self):
        return parse_interval("1m" if self.interval is None else self.interval)




    @model_validator(mode="before")
    @classmethod
    def reject_retired_bucket_parameters(cls, value):
        if isinstance(value, dict) and RETIRED_BUCKET_PARAMETERS.intersection(value):
            raise ValueError("Use interval; legacy bucket parameters are no longer supported")
        return value

    @model_validator(mode="after")
    def validate_history_query(self):
        self.resolved_interval  # Validate the interval before SQL.
        if self.interval is not None and self.aggregate is None:
            self.aggregate = "avg"
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
            self.interval is not None
            or self.aggregate is not None
        )
        if not uses_aggregation:
            return self

        # Validate that if the user provided an aggregate parameter, it must be "avg"
        if self.aggregate != "avg":
            raise ValueError("aggregate must be 'avg' when provided")

        if self.interval is None:
            raise ValueError("interval is required when aggregate is provided")

        return self


def history_query_params(
    request: Request,
    metric: list[str] | None = Query(None),
    start: str | None = Query(None), end: str | None = Query(None),
    aggregate: str | None = Query(None),
    interval: str | None = Query(None, description="Average into a fixed duration such as 5m, 1h or 24h; day is an Athens calendar day. Default: 1m."),
    limit: int = Query(100, ge=1, le=1000),
):
    try:
        if RETIRED_BUCKET_PARAMETERS.intersection(request.query_params):
            raise HTTPException(422, "Use interval; legacy bucket parameters are no longer supported")
        return HistoryQueryParams(metric=metric, start=start, end=end, aggregate=aggregate,
                                  interval=interval, limit=limit)
    except ValidationError as exc:
        raise HTTPException(422, "Invalid history query: " + str(exc.errors()[0]["msg"])) from exc
