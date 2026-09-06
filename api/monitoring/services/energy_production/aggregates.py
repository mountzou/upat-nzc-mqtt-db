"""Server-owned PV integration and daily/hourly presentation contracts.

Aggregate observed PostgreSQL samples in SQL, never the legacy zero-filled chart adapter.
Cache prepared days and requested hours; hourly entries can also serve daily reads.
Daily-only reads never fetch hourly data in advance. The mobile client receives no five-minute samples or aggregation task.
"""
from __future__ import annotations

import math
from collections import OrderedDict
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from threading import Lock
from time import monotonic
from typing import Literal

from fastapi import HTTPException
from pydantic import BaseModel

from monitoring.utils.timezone import APP_TIMEZONE


class SolarHour(BaseModel):
    start: str
    mean_power_kw: float | None
    energy_kwh: float | None
    observed_samples: int
    expected_samples: int


class SolarDay(BaseModel):
    date: str
    energy_kwh: float | None
    peak_power_kw: float | None
    productive_hours: float | None
    observed_samples: int
    expected_samples: int
    quality: Literal["complete", "partial", "missing"]
    hours: list[SolarHour]


class SolarSummary(BaseModel):
    total_energy_kwh: float | None
    mean_daily_energy_kwh: float | None
    maximum_daily_energy_kwh: float | None
    peak_power_kw: float | None
    productive_hours: float | None
    observed_day_count: int
    partial_day_count: int


class SolarProductionResponse(BaseModel):
    source: Literal["postgres"] = "postgres"
    timezone: Literal["Europe/Athens"] = "Europe/Athens"
    resolution: Literal["day", "hour"]
    start_date: str
    end_date: str
    latest_observed_at: str | None
    fetched_at: str
    days: list[SolarDay]
    summary: SolarSummary


def date_range(start: date, end: date) -> list[date]:
    count = (end - start).days + 1
    if count < 1 or count > 90 or end == date.max:
        raise ValueError("PV history must contain between 1 and 90 days")
    return [start + timedelta(days=i) for i in range(count)]


def query_days(start: date, end: date, resolution: str) -> dict[date, tuple[SolarDay, str | None]]:
    """One bounded PostgreSQL aggregation; no original samples leave the database."""
    import main
    requested = date_range(start, end)
    first = datetime.combine(start, time.min, APP_TIMEZONE).astimezone(timezone.utc)
    stop = datetime.combine(end + timedelta(days=1), time.min, APP_TIMEZONE).astimezone(timezone.utc)
    # The expression is selected from literals, never user-provided SQL.
    hour_projection = "date_trunc('hour', r.observed_at, 'UTC')" if resolution == 'hour' else "NULL::timestamptz"
    group_by = "GROUP BY GROUPING SETS ((day, hour), (day))" if resolution == 'hour' else "GROUP BY day, hour"
    sql = f"""
        WITH observations AS (
            SELECT (r.observed_at AT TIME ZONE 'Europe/Athens')::date AS day,
                   {hour_projection} AS hour, r.observed_at, r.active_power_kw AS power,
                   r.quality_status
            FROM pv_plant_readings_5m r JOIN pv_plants p ON p.id = r.plant_id
            WHERE p.site_key = %s AND r.observed_at >= %s AND r.observed_at < %s
              AND r.quality_status <> 'invalid' AND r.active_power_kw IS NOT NULL
        )
        SELECT day, hour, SUM(power) / 12.0 AS energy_kwh, AVG(power) AS mean_power_kw,
               MAX(power) AS peak_power_kw, COUNT(*) FILTER (WHERE power > 0) / 12.0 AS productive_hours,
               COUNT(*) AS observed_samples,
               BOOL_AND(quality_status IN ('complete', 'night')) AS quality_complete,
               COUNT(*) FILTER (WHERE power < 0 OR power::text IN ('NaN','Infinity','-Infinity')) AS invalid_samples,
               MAX(observed_at) AS latest
        FROM observations {group_by} ORDER BY day, hour NULLS FIRST
    """
    try:
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '15s'")
                cur.execute(sql, ('upat-pv', first, stop))
                rows = cur.fetchall()
    except Exception as exc:
        raise HTTPException(503, 'PV aggregates are temporarily unavailable') from exc
    by_day = {row['day']: row for row in rows if row['hour'] is None}
    by_hour = {row['hour']: row for row in rows if row['hour'] is not None}
    for row in rows:
        if row['invalid_samples'] or any(not math.isfinite(float(row[k])) for k in ['energy_kwh','mean_power_kw','peak_power_kw']):
            raise HTTPException(502, 'Invalid PV aggregate source data')
    result = {}
    for day in requested:
        first = datetime.combine(day, time.min, APP_TIMEZONE).astimezone(timezone.utc)
        stop = datetime.combine(day + timedelta(days=1), time.min, APP_TIMEZONE).astimezone(timezone.utc)
        expected = int((stop - first).total_seconds() / 300)
        row = by_day.get(day)
        observed = int(row['observed_samples']) if row else 0
        complete = bool(row and observed == expected and row['quality_complete'])
        hours = []
        if resolution == 'hour':
            cursor = first
            while cursor < stop:
                h = by_hour.get(cursor)
                hours.append(SolarHour(start=cursor.isoformat().replace('+00:00','Z'),
                    mean_power_kw=float(h['mean_power_kw']) if h else None,
                    energy_kwh=float(h['energy_kwh']) if h else None,
                    observed_samples=int(h['observed_samples']) if h else 0, expected_samples=12))
                cursor += timedelta(hours=1)
        result[day] = (SolarDay(date=day.isoformat(),energy_kwh=float(row['energy_kwh']) if row else None,
            peak_power_kw=float(row['peak_power_kw']) if row else None,
            productive_hours=float(row['productive_hours']) if row else None,
            observed_samples=observed,expected_samples=expected,
            quality='complete' if complete else 'partial' if observed else 'missing',hours=hours),
            row['latest'].astimezone(timezone.utc).isoformat().replace('+00:00','Z') if row else None)
    return result


@dataclass
class PreparedDay:
    data: SolarDay
    latest: str | None
    fetched_at: str
    expires: float


class SolarProductionCache:
    """Bounded, per-process day cache with overlapping-request coalescing.

    Both resolutions reuse the same entries. Futures are registered under the
    lock, but neither I/O nor waiting on another request holds the lock.
    """
    def __init__(self, capacity=180, ttl=300, clock=monotonic):
        self.capacity, self.ttl, self.clock = capacity, ttl, clock
        self.entries: OrderedDict[date, PreparedDay] = OrderedDict()
        self.inflight: dict[tuple[date, str], Future] = {}
        self.lock = Lock()

    def get(self, start: date, end: date, resolution: str = "day") -> list[PreparedDay]:
        dates = date_range(start, end)
        collected = {}
        while len(collected) < len(dates):
            with self.lock:
                for day in dates:
                    entry = self.entries.get(day)
                    if day not in collected and entry and entry.expires > self.clock() and (resolution == "day" or entry.data.hours):
                        collected[day] = entry
                        self.entries.move_to_end(day)
                remaining = [d for d in dates if d not in collected]
                if not remaining:
                    break
                first = remaining[0]
                future = self.inflight.get((first, "hour")) or self.inflight.get((first, resolution))
                owned = []
                if future is None:
                    for day in dates[dates.index(first):]:
                        if day in collected or (day, resolution) in self.inflight or (day, "hour") in self.inflight:
                            break
                        owned.append(day)
                    future = Future()
                    for day in owned:
                        self.inflight[(day, resolution)] = future
            if owned:
                try:
                    prepared = query_days(owned[0], owned[-1], resolution)
                    fetched_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                    entries = {day: PreparedDay(data, latest, fetched_at, self.clock() + self.ttl)
                               for day, (data, latest) in prepared.items()}
                    with self.lock:
                        self.entries.update(entries)
                        for day in entries:
                            self.entries.move_to_end(day)
                        while len(self.entries) > self.capacity:
                            self.entries.popitem(last=False)
                    future.set_result(entries)
                except Exception as exc:
                    future.set_exception(exc)
                finally:
                    with self.lock:
                        for day in owned:
                            self.inflight.pop((day, resolution), None)
            fetched = future.result()
            collected.update({d: entry for d, entry in fetched.items() if d in dates})
        return [collected[d] for d in dates]


solar_production_cache = SolarProductionCache()


def get_solar_production(start: date, end: date, resolution: Literal["day", "hour"]) -> SolarProductionResponse:
    entries = solar_production_cache.get(start, end, resolution)
    days = [entry.data for entry in entries]
    measured = [day for day in days if day.energy_kwh is not None]
    total = math.fsum(day.energy_kwh for day in measured) if measured else None
    if total is not None and not math.isfinite(total):
        raise HTTPException(502, "Invalid PV aggregate source data")
    return SolarProductionResponse(
        resolution=resolution, start_date=start.isoformat(), end_date=end.isoformat(),
        latest_observed_at=max((e.latest for e in entries if e.latest), default=None),
        # Oldest constituent fetch: composing a cached response cannot reset freshness.
        fetched_at=min(e.fetched_at for e in entries),
        days=[day.model_copy(update={"hours": []}) for day in days] if resolution == "day" else days,
        summary=SolarSummary(
            total_energy_kwh=total, mean_daily_energy_kwh=total / len(measured) if measured else None,
            maximum_daily_energy_kwh=max((day.energy_kwh for day in measured), default=None),
            peak_power_kw=max((day.peak_power_kw for day in measured), default=None),
            productive_hours=math.fsum(day.productive_hours for day in measured) if measured else None,
            observed_day_count=len(measured), partial_day_count=sum(day.quality == "partial" for day in days),
        ),
    )
