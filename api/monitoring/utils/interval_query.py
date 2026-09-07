"""Canonical interval HTTP validation."""
from fastapi import HTTPException, Query, Request
from monitoring.utils.interval import parse_interval, RETIRED_BUCKET_PARAMETERS

DESCRIPTION = "Fixed elapsed duration, e.g. 5m, 1h or 24h. 'day' means a calendar day in Europe/Athens (23, 24 or 25 hours)."


def history_interval(
    request: Request,
    interval: str | None = Query(None, description=DESCRIPTION + " Defaults to 1h."),
) -> str:
    try:
        if RETIRED_BUCKET_PARAMETERS.intersection(request.query_params):
            raise ValueError("Use interval; legacy bucket parameters are no longer supported")
        if interval is not None and any(request.query_params.get(flag, "").lower() in {"true", "1", "yes", "on"} for flag in ("rolling_1h", "rolling_24h_hourly")):
            raise ValueError("Use interval or a rolling preset, not both")
        return parse_interval("1h" if interval is None else interval).value
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
