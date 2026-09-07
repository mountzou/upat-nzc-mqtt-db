"""PV energy contracts. Integration and statistics stay on the server.

PostgreSQL reduces observed five-minute power / forecast hourly power to energy.
Prepared days are bounded, shared across users (the PV installation is shared),
and coalesced across overlapping requests. No raw power leaves this service.
"""
from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from math import fsum, isfinite
from threading import Lock
from time import monotonic
from typing import Literal

from fastapi import HTTPException
from pydantic import BaseModel, Field

from monitoring.utils.interval import ATHENS, ORIGIN, Interval, parse_interval

Kind = Literal['history', 'forecast']
Quality = Literal['complete', 'partial', 'missing']


def date_range(start: date, end: date) -> list[date]:
    count = (end - start).days + 1
    if count < 1 or count > 90 or end == date.max:
        raise ValueError("PV history must contain between 1 and 90 days")
    return [start + timedelta(days=i) for i in range(count)]


class EnergyBucket(BaseModel):
    start: str
    end: str
    energy_kwh: float | None
    observed_samples: int
    expected_samples: int
    quality: Quality


class EnergyDay(BaseModel):
    date: str
    energy_kwh: float | None
    productive_hours: float | None
    observed_samples: int
    expected_samples: int
    quality: Quality


class EnergySummary(BaseModel):
    total_energy_kwh: float | None
    mean_daily_energy_kwh: float | None
    maximum_daily_energy_kwh: float | None
    maximum_interval_energy_kwh: float | None
    productive_hours: float | None
    observed_day_count: int
    partial_day_count: int
    missing_day_count: int


class HourProfile(BaseModel):
    hour: int
    energy_kwh: float | None
    share_pct: float | None
    observed_samples: int


class ForecastRun(BaseModel):
    run_id: int
    forecast_date: str
    generated_at: str


class ProductionEnergyResponse(BaseModel):
    source: Literal['postgres'] = 'postgres'
    kind: Kind
    unit: Literal['kWh'] = 'kWh'
    timezone: Literal['Europe/Athens'] = 'Europe/Athens'
    interval: str
    start_date: str
    end_date: str
    latest_observed_at: str | None
    fetched_at: str
    points: list[EnergyBucket]
    days: list[EnergyDay]
    summary: EnergySummary
    hourly_profile: list[HourProfile]
    runs: list[ForecastRun] = Field(default_factory=list)


def instant(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')


def midnight(day: date) -> datetime:
    return datetime.combine(day, time.min, ATHENS).astimezone(timezone.utc)


def energy_interval(value: str, kind: Kind) -> Interval:
    interval = parse_interval(value)
    step = 5 if kind == 'history' else 60
    if not interval.calendar_day and (interval.minutes % step or interval.minutes > 10080):
        raise ValueError(f'{kind} interval must be a multiple of {step}m up to 168h, or day')
    return interval


def quality(observed: int, expected: int, complete: bool = True) -> Quality:
    return 'missing' if not observed else 'complete' if observed == expected and complete else 'partial'


@dataclass
class PreparedDay:
    rows: list[dict]
    fetched_at: str
    expires: float


def query_days(start: date, end: date, interval: Interval, kind: Kind) -> dict[date, list[dict]]:
    """One bounded SQL scan produces bucket, day and clock-hour energy statistics."""
    import main
    requested = date_range(start, end)
    first, stop = midnight(start), midnight(end + timedelta(days=1))
    step = 300 if kind == 'history' else 3600
    params: list = [start, end] if kind == 'forecast' else ['upat-pv', first, stop]
    if kind == 'history':
        source = """
            SELECT r.observed_at AS ts, r.active_power_kw::double precision AS power,
                   r.quality_status IN ('complete','night') AS complete,
                   NULL::integer AS run_id, NULL::timestamptz AS generated_at
            FROM pv_plant_readings_5m r JOIN pv_plants p ON p.id = r.plant_id
            WHERE p.site_key = %s AND r.observed_at >= %s AND r.observed_at < %s
              AND r.quality_status <> 'invalid' AND r.active_power_kw IS NOT NULL
        """
    else:
        # Forecast persistence has a distinct, explicit local-civil TIMESTAMP
        # contract. Reject nonexistent spring hours by round-tripping; do not
        # invent the missing repeated hour in legacy 24-row autumn forecasts.
        source = """
            WITH selected AS (
                SELECT DISTINCT ON (forecast_date) id, forecast_date,
                       COALESCE(completed_at, started_at) AS generated_at
                FROM pv_day_ahead_forecast_runs
                WHERE success = TRUE AND forecast_date BETWEEN %s AND %s
                ORDER BY forecast_date, started_at DESC, id DESC
            )
            SELECT h.forecast_timestamp AT TIME ZONE 'Europe/Athens' AS ts,
                   h.predicted_power_kw::double precision AS power, TRUE AS complete,
                   r.id AS run_id, r.generated_at
            FROM selected r JOIN pv_day_ahead_forecast_hourly h ON h.run_id = r.id
            WHERE h.forecast_timestamp::date = r.forecast_date
              AND (h.forecast_timestamp AT TIME ZONE 'Europe/Athens')
                   AT TIME ZONE 'Europe/Athens' = h.forecast_timestamp
        """
    bucket_sql = "date_trunc('day', ts, 'Europe/Athens')" if interval.calendar_day else "date_bin(%s::interval, ts, %s::timestamptz)"
    if not interval.calendar_day:
        params.extend([interval.sql_duration, ORIGIN])
    sql = f"""
        WITH source AS MATERIALIZED ({source}), observations AS (
            SELECT *, (ts AT TIME ZONE 'Europe/Athens')::date AS day,
                   {bucket_sql} AS bucket,
                   EXTRACT(hour FROM ts AT TIME ZONE 'Europe/Athens')::integer AS clock_hour
            FROM source
        )
        SELECT day, bucket, clock_hour, GROUPING(bucket) AS gb, GROUPING(clock_hour) AS gh,
               SUM(power) * {step}/3600.0 AS energy_kwh,
               COUNT(*) FILTER (WHERE power > 0) * {step}/3600.0 AS productive_hours,
               COUNT(*) AS observed_samples, BOOL_AND(complete) AS complete,
               COUNT(*) FILTER (WHERE power < 0 OR power::text IN ('NaN','Infinity','-Infinity')
                    OR MOD(EXTRACT(epoch FROM ts), {step}) <> 0) AS invalid_samples,
               MAX(ts) AS latest, MAX(run_id) AS run_id, MAX(generated_at) AS generated_at
        FROM observations
        GROUP BY GROUPING SETS ((day,bucket),(day),(day,clock_hour))
        ORDER BY day, bucket, clock_hour
    """
    try:
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '15s'")
                cur.execute(sql, params)
                rows = cur.fetchall()
    except Exception as exc:
        raise HTTPException(503, 'PV energy is temporarily unavailable') from exc
    result = {day: [] for day in requested}
    for row in rows:
        if row['invalid_samples'] or not isfinite(float(row['energy_kwh'])):
            raise HTTPException(502, 'Invalid PV energy source data')
        result[row['day']].append(row)
    return result


class EnergyDayCache:
    """Bounded 5-minute cache. Overlapping requests share in-flight day work.

    A cached hourly read also supplies daily reads, without resetting freshness.
    Keys include source kind and normalized interval; no user data is cached.
    """
    def __init__(self, capacity=360, ttl=300, clock=monotonic):
        self.capacity, self.ttl, self.clock = capacity, ttl, clock
        self.entries = OrderedDict()
        self.inflight: dict[tuple, Future] = {}
        self.lock = Lock()

    def get(self, start: date, end: date, interval: Interval, kind: Kind) -> dict[date, PreparedDay]:
        dates, collected = date_range(start, end), {}
        key = lambda d: (kind, d, interval.value)
        while len(collected) < len(dates):
            with self.lock:
                for day in dates:
                    candidates = [key(day)] + ([(kind, day, '1h')] if interval.calendar_day else [])
                    for candidate in candidates:
                        entry = self.entries.get(candidate)
                        if day not in collected and entry and entry.expires > self.clock():
                            collected[day] = entry
                            self.entries.move_to_end(candidate)
                remaining = [d for d in dates if d not in collected]
                if not remaining:
                    break
                first = remaining[0]
                future = self.inflight.get(key(first))
                if future is None and interval.calendar_day:
                    future = self.inflight.get((kind, first, '1h'))
                owned = []
                if future is None:
                    for day in dates[dates.index(first):]:
                        if day in collected or key(day) in self.inflight:
                            break
                        owned.append(day)
                    future = Future()
                    for day in owned:
                        self.inflight[key(day)] = future
            if owned:
                try:
                    rows = query_days(owned[0], owned[-1], interval, kind)
                    fetched = instant(datetime.now(timezone.utc))
                    prepared = {d: PreparedDay(r, fetched, self.clock()+self.ttl) for d, r in rows.items()}
                    with self.lock:
                        for day, entry in prepared.items():
                            self.entries[key(day)] = entry
                            self.entries.move_to_end(key(day))
                        while len(self.entries) > self.capacity:
                            self.entries.popitem(last=False)
                    future.set_result(prepared)
                except Exception as exc:
                    future.set_exception(exc)
                finally:
                    with self.lock:
                        for day in owned:
                            self.inflight.pop(key(day), None)
            collected.update({d: e for d, e in future.result().items() if d in dates})
        return {d: collected[d] for d in dates}


energy_cache = EnergyDayCache()


def latest_forecast_date() -> date:
    import main
    try:
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '5s'")
                cur.execute('SELECT forecast_date FROM pv_day_ahead_forecast_runs WHERE success=TRUE ORDER BY started_at DESC, id DESC LIMIT 1')
                row = cur.fetchone()
    except Exception as exc:
        raise HTTPException(503, 'PV forecasts are temporarily unavailable') from exc
    if not row:
        raise HTTPException(404, 'No stored PV forecast found')
    return row['forecast_date']


def get_energy(start: date, end: date, value: str = '1h', kind: Kind = 'history') -> ProductionEnergyResponse:
    interval = energy_interval(value, kind)
    entries = energy_cache.get(start, end, interval, kind)
    first, stop = midnight(start), midnight(end + timedelta(days=1))
    step = 300 if kind == 'history' else 3600
    days, latest, runs = [], [], []
    bucket_rows: dict[datetime, list[dict]] = {}
    profiles: dict[int, list[dict]] = {h: [] for h in range(24)}
    for day, entry in entries.items():
        daily = next((r for r in entry.rows if r['gb'] and r['gh']), None)
        expected = int((midnight(day+timedelta(days=1))-midnight(day)).total_seconds()/step)
        observed = int(daily['observed_samples']) if daily else 0
        if observed > expected:
            raise HTTPException(502, 'Duplicate PV energy source intervals')
        days.append(EnergyDay(date=str(day), energy_kwh=float(daily['energy_kwh']) if daily else None,
            productive_hours=float(daily['productive_hours']) if daily else None,
            observed_samples=observed, expected_samples=expected,
            quality=quality(observed, expected, daily['complete'] if daily else False)))
        if daily:
            latest.append(daily['latest'])
            if daily['run_id'] is not None:
                runs.append(ForecastRun(run_id=daily['run_id'], forecast_date=str(day), generated_at=instant(daily['generated_at'])))
            if interval.calendar_day:
                bucket_rows[midnight(day)] = [daily]
        for row in entry.rows:
            if not interval.calendar_day and not row['gb']:
                bucket_rows.setdefault(row['bucket'], []).append(row)
            if not row['gh']:
                profiles[row['clock_hour']].append(row)
    points = []
    cursor = interval.floor(first)
    while cursor < stop:
        next_cursor = interval.shift(cursor, 1)
        left, right = max(cursor, first), min(next_cursor, stop)
        rows = bucket_rows.get(cursor, [])
        count = sum(int(r['observed_samples']) for r in rows)
        expected = int((right-left).total_seconds()/step)
        if count > expected:
            raise HTTPException(502, 'Duplicate PV energy source intervals')
        points.append(EnergyBucket(start=instant(left), end=instant(right),
            energy_kwh=fsum(float(r['energy_kwh']) for r in rows) if rows else None,
            observed_samples=count, expected_samples=expected,
            quality=quality(count, expected, all(r['complete'] for r in rows))))
        cursor = next_cursor
    measured = [d for d in days if d.energy_kwh is not None]
    total = fsum(d.energy_kwh for d in measured) if measured else None
    profile = []
    for hour, rows in profiles.items():
        energy = fsum(float(r['energy_kwh']) for r in rows) if rows else None
        profile.append(HourProfile(hour=hour, energy_kwh=energy,
            share_pct=energy/total*100 if total and energy is not None else 0 if energy is not None else None,
            observed_samples=sum(int(r['observed_samples']) for r in rows)))
    return ProductionEnergyResponse(kind=kind, interval=interval.value, start_date=str(start), end_date=str(end),
        latest_observed_at=instant(max(latest)) if latest and kind == 'history' else None,
        fetched_at=min(e.fetched_at for e in entries.values()), points=points, days=days,
        summary=EnergySummary(total_energy_kwh=total,
            mean_daily_energy_kwh=total/len(measured) if measured else None,
            maximum_daily_energy_kwh=max((d.energy_kwh for d in measured), default=None),
            maximum_interval_energy_kwh=max((p.energy_kwh for p in points if p.energy_kwh is not None), default=None),
            productive_hours=fsum(d.productive_hours for d in measured) if measured else None,
            observed_day_count=len(measured), partial_day_count=sum(d.quality == 'partial' for d in days),
            missing_day_count=sum(d.quality == 'missing' for d in days)), hourly_profile=profile, runs=runs)
