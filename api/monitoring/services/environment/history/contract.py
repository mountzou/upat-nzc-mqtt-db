"""A limit caps requested buckets; it never changes or truncates a time range."""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re

from fastapi import HTTPException, Query, Request
from pydantic import AwareDatetime

from monitoring.utils.interval import Interval, parse_interval, RETIRED_BUCKET_PARAMETERS
from monitoring.utils.timezone import utc_now

DEFAULT_METRICS = ('temperature', 'relative_humidity', 'co2', 'voc', 'pm25')
RETIRED = RETIRED_BUCKET_PARAMETERS | {'aggregate', 'rolling_1h', 'rolling_24h_hourly'}
MAX_RANGE = timedelta(days=366)

@dataclass(frozen=True)
class HistoryPlan:
    start: datetime
    end: datetime
    interval: Interval
    alignment: str
    metrics: tuple[str, ...]
    limit: int
    buckets: tuple[tuple[datetime, datetime], ...]


def resolve_history_plan(*, start=None, end=None, window=None, interval='1h',
                         alignment='clock', metric=None, limit=1000, now=None):
    try:
        spec = parse_interval(interval)
        if alignment not in {'window', 'clock'}:
            raise ValueError('alignment must be window or clock')
        if alignment == 'clock' and spec.value not in {'1h', 'day'}:
            raise ValueError('clock alignment supports 1h or an Athens calendar day')
        if alignment == 'window' and spec.calendar_day:
            raise ValueError('day requires clock alignment; use 24h for elapsed time')
        if not 1 <= limit <= 1000:
            raise ValueError('limit must be between 1 and 1000')
        if (start is None) != (end is None):
            raise ValueError('start and end must be supplied together')
        if start is not None and window is not None:
            raise ValueError('Use start/end or window, not both')
        if start is None:
            # A missing window has one fixed meaning, independent of limit.
            duration = parse_interval('24h' if window is None else window)
            if duration.calendar_day:
                raise ValueError('window must be an elapsed duration such as 1h or 24h; use start/end for calendar ranges')
            if duration.minutes > 366 * 24 * 60:
                raise ValueError('history range cannot exceed 366 days')
            end = now or utc_now()
            if end.tzinfo is None or end.utcoffset() is None:
                raise ValueError('now must be an aware timestamp')
            end = end.astimezone(timezone.utc)
            if alignment == 'clock':
                end = spec.floor(end)
            start = end - timedelta(minutes=duration.minutes)
        for stamp in (start, end):
            if stamp.tzinfo is None or stamp.utcoffset() is None:
                raise ValueError('start/end must include a UTC offset')
        start, end = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
        if not start < end:
            raise ValueError('start must be before end')
        if end - start > MAX_RANGE:
            raise ValueError('history range cannot exceed 366 days')
        if alignment == 'clock':
            if start != spec.floor(start) or end != spec.floor(end):
                raise ValueError('clock start/end must match complete bucket boundaries; use alignment=window for arbitrary instants')
            if end > spec.floor(now or utc_now()):
                raise ValueError('clock analytics cannot include an unfinished bucket')
        metrics = tuple(dict.fromkeys(metric if metric is not None else DEFAULT_METRICS))
        if not metrics or len(metrics) > 32 or any(not re.fullmatch(r'[a-z][a-z0-9_]{0,63}', m) for m in metrics):
            raise ValueError('metric must contain 1 to 32 non-empty metric names')
        buckets = []
        cursor = start
        while cursor < end:
            if len(buckets) == limit:
                raise ValueError('requested range exceeds limit; shorten the range or increase interval/limit; no data was truncated')
            next_end = min(spec.shift(cursor, 1), end)
            buckets.append((cursor, next_end))
            cursor = next_end
        return HistoryPlan(start, end, spec, alignment, metrics, limit, tuple(buckets))
    except (ValueError, OverflowError) as exc:
        raise HTTPException(422, str(exc)) from exc


def history_plan(request: Request,
                 start: AwareDatetime | None = Query(None, description='Inclusive instant; requires end.'),
                 end: AwareDatetime | None = Query(None, description='Exclusive instant; requires start.'),
                 window: str | None = Query(None, description='Elapsed lookback, e.g. 1h or 24h; excludes start/end. Defaults to 24h.'),
                 interval: str = Query('1h'),
                 alignment: str = Query('clock', description='window anchors buckets at start; clock uses complete 1h/Athens day buckets.'),
                 metric: list[str] | None = Query(None),
                 limit: int = Query(1000, ge=1, le=1000, description='Maximum requested buckets including gaps; oversized ranges return 422, never truncated.')):
    params = request.query_params
    if RETIRED.intersection(params):
        raise HTTPException(422, 'Use window or start/end with interval and alignment; aggregate and rolling/bucket parameters are retired')
    allowed = {'start', 'end', 'window', 'interval', 'alignment', 'metric', 'limit'}
    if set(params) - allowed:
        raise HTTPException(422, 'Unsupported environmental history parameter')
    if any(len(params.getlist(k)) > 1 for k in allowed - {'metric'}):
        raise HTTPException(422, 'Only metric may be repeated')
    return resolve_history_plan(start=start, end=end, window=window, interval=interval,
                                alignment=alignment, metric=metric, limit=limit)
