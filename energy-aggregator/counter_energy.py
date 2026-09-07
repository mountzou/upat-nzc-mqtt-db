"""Conservative, deterministic hourly energy from cumulative counters.

Adjacent hours share the same nearest boundary observation. No interpolation,
extrapolation, power fallback, or splitting a delta across an unobserved gap.
"""
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
import math

METHOD = 'counter_delta_v1'
BOUNDARY_TOLERANCE_SECONDS = 60
HOUR = timedelta(hours=1)


@dataclass(frozen=True)
class Sample:
    observed_at: datetime
    energy_wh: float
    returned_energy_wh: float | None
    counter_kind: str


@dataclass(frozen=True)
class Result:
    energy_wh: float | None
    reason: str
    sample_count: int = 0
    max_gap_seconds: float | None = None
    start_offset_seconds: float | None = None
    end_offset_seconds: float | None = None

    def details(self):
        return {k: v for k, v in asdict(self).items() if k not in ('energy_wh', 'reason')}


def utc(value):
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError('An explicit timezone offset is required')
    return value.astimezone(timezone.utc)


def hourly_energy(samples, start, end):
    start, end = utc(start), utc(end)
    if end - start != HOUR or start.minute or start.second or start.microsecond:
        raise ValueError('Expected one aligned elapsed hour')
    tolerance = timedelta(seconds=BOUNDARY_TOLERANCE_SECONDS)
    by_time = {}
    conflicts = set()
    for s in samples:
        ts = utc(s.observed_at)
        if not start-tolerance <= ts <= end+tolerance:
            continue
        if ts in by_time and by_time[ts] != s:
            conflicts.add(ts)
        by_time[ts] = s
    times = sorted(by_time)
    if len(times) < 2:
        return Result(None, 'insufficient_samples', len(times))
    first = min(times, key=lambda t: (abs(t-start), t))
    last = min(times, key=lambda t: (abs(t-end), t))
    if abs(first-start) > tolerance or abs(last-end) > tolerance:
        return Result(None, 'missing_boundary', len(times))
    selected = [t for t in times if first <= t <= last]
    gap = max((b-a).total_seconds() for a, b in zip(selected, selected[1:]))
    info = dict(sample_count=len(selected), max_gap_seconds=gap,
                start_offset_seconds=(first-start).total_seconds(),
                end_offset_seconds=(last-end).total_seconds())
    if conflicts.intersection(selected):
        return Result(None, 'conflicting_timestamp', **info)
    series = [by_time[t] for t in selected]
    shape = (series[0].counter_kind, series[0].returned_energy_wh is not None)
    for s in series:
        if (s.counter_kind, s.returned_energy_wh is not None) != shape:
            return Result(None, 'counter_shape_changed', **info)
        values = [s.energy_wh] + ([s.returned_energy_wh] if s.returned_energy_wh is not None else [])
        if s.counter_kind not in ('import', 'absolute') or any(
            isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0
            for v in values
        ):
            return Result(None, 'invalid_counter', **info)
        if s.counter_kind == 'absolute' and (s.returned_energy_wh or 0) > s.energy_wh:
            return Result(None, 'invalid_counter', **info)
    for a, b in zip(series, series[1:]):
        if b.energy_wh < a.energy_wh or (a.returned_energy_wh is not None and
                                        b.returned_energy_wh < a.returned_energy_wh):
            return Result(None, 'counter_reset', **info)
        if shape[0] == 'absolute' and (b.energy_wh-a.energy_wh) < (
            (b.returned_energy_wh or 0)-(a.returned_energy_wh or 0)
        ):
            return Result(None, 'invalid_import_delta', **info)
    delta = series[-1].energy_wh-series[0].energy_wh
    if shape[0] == 'absolute':
        delta -= (series[-1].returned_energy_wh or 0)-(series[0].returned_energy_wh or 0)
    return Result(round(delta, 3), 'observed', **info)
