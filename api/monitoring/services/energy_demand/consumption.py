"""Consumption from persisted hourly energy, with a catalog-defined source plan.

No raw-power integration, temporal interpolation or coverage extrapolation occurs
here. Each selected plug/phase contributes once. A school meter never falls back
to its submeters merely because its readings are missing.
"""
from __future__ import annotations

from collections import OrderedDict, defaultdict
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from math import fsum
from threading import Lock
from time import monotonic
from typing import Literal

from fastapi import HTTPException
from psycopg2.extras import Json
from pydantic import BaseModel

from monitoring.policies.school_hours import is_local_school_hour
from monitoring.services.energy_demand.devices import list_energy_devices_for_school, is_whole_building_energy_meter
from monitoring.services.service_energy_api import _emissions_factor_contract
from monitoring.utils.interval import ATHENS, Interval, parse_interval

HOUR = timedelta(hours=1)
UTC = timezone.utc
Quality = Literal['complete', 'partial', 'missing']


class Coverage(BaseModel):
    expected_hours: int
    observed_hours: int
    complete_hours: int
    expected_series: int
    expected_series_hours: int
    observed_series_hours: int
    quality: Quality


class ConsumptionPoint(BaseModel):
    start: str
    end: str
    energy_kwh: float | None
    equivalent_co2_kg: float | None
    outside_school_hours_kwh: float | None
    observed_series_hours: int
    expected_series_hours: int
    quality: Quality
    # Values correspond to breakdown order; load metadata is sent only once.
    load_energy_kwh: list[float | None]
    load_equivalent_co2_kg: list[float | None]


class ConsumptionLoad(BaseModel):
    id: str
    label: str
    room_id: str
    phases: list[str]
    energy_kwh: float | None
    observed_series_hours: int
    expected_series_hours: int
    quality: Quality


class ConsumptionSummary(BaseModel):
    total_energy_kwh: float | None
    equivalent_co2_kg: float | None
    mean_hourly_energy_kwh: float | None
    peak_hourly_energy_kwh: float | None
    maximum_interval_energy_kwh: float | None
    during_school_hours_kwh: float | None
    outside_school_hours_kwh: float | None
    outside_school_hours_pct: float | None
    measured_hour_count: int


class ConsumptionDay(BaseModel):
    date: str
    energy_kwh: float | None
    during_school_hours_kwh: float | None
    outside_school_hours_kwh: float | None
    outside_school_hours_pct: float | None
    is_weekend: bool
    coverage: Coverage


class ConsumptionHourProfile(BaseModel):
    hour: int
    mean_energy_kwh: float | None
    share_pct: float | None
    observed_hours: int


class ConsumptionHistory(BaseModel):
    contract: Literal['consumption.history.v1'] = 'consumption.history.v1'
    school_id: str
    room_id: str | None
    source_basis: Literal['central_meter', 'submeter_sum']
    measurement_scope: Literal['whole_school', 'assumed_school_total', 'metered_loads']
    unit: Literal['kWh'] = 'kWh'
    timezone: Literal['Europe/Athens'] = 'Europe/Athens'
    interval: str
    start: str
    end: str
    fetched_at: str
    emissions_factor_kg_per_kwh: float
    emissions_factor_source: str
    emissions_factor_reference_year: int | None
    emissions_factor_version: str
    points: list[ConsumptionPoint]
    summary: ConsumptionSummary
    coverage: Coverage
    breakdown: list[ConsumptionLoad]
    days: list[ConsumptionDay]
    hourly_profile: list[ConsumptionHourProfile]


def instant(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace('+00:00', 'Z')


def bounds(start_date: date | None, end_date: date | None,
           start: datetime | None, end: datetime | None) -> tuple[datetime, datetime]:
    if start_date is not None or end_date is not None:
        if start_date is None or end_date is None or start is not None or end is not None:
            raise ValueError('Use either start_date/end_date or start/end, as a complete pair')
        if not 1 <= (end_date - start_date).days + 1 <= 90 or end_date == date.max:
            raise ValueError('Request between 1 and 90 Athens calendar days')
        start = datetime.combine(start_date, time.min, ATHENS)
        end = datetime.combine(end_date + timedelta(days=1), time.min, ATHENS)
    if start is None or end is None or start.tzinfo is None or end.tzinfo is None:
        raise ValueError('Provide date bounds or offset-aware start/end instants')
    start, end = start.astimezone(UTC), end.astimezone(UTC)
    if start >= end or end - start > timedelta(days=90, hours=1):
        raise ValueError('Request a positive range of at most 90 days (including the DST hour)')
    if any(t.minute or t.second or t.microsecond for t in (start, end)):
        raise ValueError('start/end must be aligned to whole hours; hourly energy cannot be prorated')
    return start, end


def consumption_interval(value: str) -> Interval:
    result = parse_interval(value)
    if not result.calendar_day and (result.minutes % 60 or not 60 <= result.minutes <= 10080):
        raise ValueError('interval must be a whole-hour multiple from 1h to 168h, or day')
    return result


@dataclass(frozen=True)
class Source:
    device_id: str
    phase: str | None
    load_id: str
    label: str
    room_id: str


def source_plan(school_id: str, room_id: str | None):
    devices = list_energy_devices_for_school(school_id)
    central = [d for d in devices if is_whole_building_energy_meter(d)]
    if len(central) > 1:
        raise HTTPException(409, 'School meter assignment is ambiguous')
    if len({d['id'] for d in devices}) != len(devices):
        raise HTTPException(409, 'Duplicate energy device assignment')
    selected = central if room_id is None and central else devices
    sources: list[Source] = []
    for device in selected:
        if device['type'] == 'plug':
            if room_id is None or device['room_id'] == room_id:
                sources.append(Source(device['id'], None, device['id'], device['label'], device['room_id']))
        else:
            for phase in ('a', 'b', 'c'):
                metadata = device['phases'][phase]
                target = metadata['target_room_id']
                if room_id is not None and target != room_id:
                    continue
                label = metadata.get('display_label') or f"{target} {metadata['load_type']}"
                # Identical phase labels on the same meter/room describe one load.
                load_id = f"{device['id']}::{target}::{metadata['load_type']}::{label}"
                sources.append(Source(device['id'], phase, load_id, label, target))
    if not sources:
        raise HTTPException(404, 'No energy meters are assigned to this scope')
    basis = 'central_meter' if room_id is None and central else 'submeter_sum'
    # Explicit project assumption, configured independently of data availability.
    from monitoring.config import DIR_ROOMS
    from monitoring.services.catalog_loader import load_validated_json_catalog
    from pydantic import TypeAdapter
    assumptions = load_validated_json_catalog(
        DIR_ROOMS / 'energy_consumption_assumptions.json',
        TypeAdapter(dict[str, Literal['assumed_school_total']]), catalog_name='consumption scope assumptions')
    scope = 'whole_school' if basis == 'central_meter' else (
        assumptions.get(school_id, 'metered_loads') if room_id is None else 'metered_loads')
    return tuple(sources), basis, scope


def query_hourly(sources: tuple[Source, ...], start: datetime, end: datetime) -> list[dict]:
    """One bounded round trip to hourly tables; never scan raw measurements.

    The primary-key prefix device_id/window_start bounds both branches. NULL,
    negative, non-finite and malformed hourly records remain unobserved.
    """
    import main
    plan = [dict(idx=i, device_id=s.device_id, phase=s.phase) for i, s in enumerate(sources)]
    sql = """
      WITH plan AS (SELECT * FROM jsonb_to_recordset(%s::jsonb)
        AS p(idx integer, device_id text, phase text)), readings AS (
        SELECT p.idx, r.window_start, r.window_end, r.energy_wh AS wh
        FROM plan p JOIN shelly_plug_hourly_energy r ON r.device_id=p.device_id
        WHERE p.phase IS NULL AND r.window_start >= %s AND r.window_start < %s
        UNION ALL
        SELECT p.idx, r.window_start, r.window_end,
          CASE p.phase WHEN 'a' THEN r.a_energy_wh WHEN 'b' THEN r.b_energy_wh ELSE r.c_energy_wh END
        FROM plan p JOIN shelly_pro3em_hourly_energy r ON r.device_id=p.device_id
        WHERE p.phase IS NOT NULL AND r.window_start >= %s AND r.window_start < %s
      )
      SELECT idx, window_start, wh / 1000.0 AS energy_kwh FROM readings
      WHERE window_end = window_start + INTERVAL '1 hour' AND window_end <= %s
        AND window_start = date_bin(INTERVAL '1 hour', window_start, TIMESTAMPTZ '2001-01-01 00:00Z')
        AND wh >= 0 AND wh < 'Infinity'::double precision
      ORDER BY window_start, idx
    """
    try:
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (Json(plan), start, end, start, end, end))
                return list(cur.fetchall())
    except Exception:
        raise HTTPException(503, 'Consumption history is temporarily unavailable') from None


def quality(observed: int, expected: int) -> str:
    return 'missing' if observed == 0 else 'complete' if observed == expected else 'partial'


def prepare(sources, basis, scope, school_id, room_id, start, end, interval):
    rows = query_hourly(sources, start, end)
    hours = [start + i * HOUR for i in range(int((end-start)/HOUR))]
    values = {h: {} for h in hours}
    for row in rows:
        h, idx = row['window_start'].astimezone(UTC), row['idx']
        if idx in values[h]:
            raise HTTPException(503, 'Duplicate hourly energy records')
        values[h][idx] = float(row['energy_kwh'])
    loads = list(dict.fromkeys(s.load_id for s in sources))
    load_members = [[i for i, s in enumerate(sources) if s.load_id == load] for load in loads]
    emission = _emissions_factor_contract()
    factor = emission['emissions_factor_kg_per_kwh']

    def total(selected, members=range(len(sources))):
        energies = [values[h][i] for h in selected for i in members if i in values[h]]
        return fsum(energies) if energies else None

    def coverage(selected):
        expected = len(selected)*len(sources)
        observed = sum(len(values[h]) for h in selected)
        return dict(expected_hours=len(selected), observed_hours=sum(bool(values[h]) for h in selected),
                    complete_hours=sum(len(values[h]) == len(sources) for h in selected),
                    expected_series=len(sources), expected_series_hours=expected,
                    observed_series_hours=observed, quality=quality(observed, expected))

    def school_summary(selected):
        inside_hours = [h for h in selected if is_local_school_hour(h.astimezone(ATHENS))]
        outside_hours = [h for h in selected if not is_local_school_hour(h.astimezone(ATHENS))]
        inside = total(inside_hours) if inside_hours else 0.0
        outside = total(outside_hours) if outside_hours else 0.0
        energy = total(selected)
        return dict(during_school_hours_kwh=inside, outside_school_hours_kwh=outside,
                    outside_school_hours_pct=(100*(outside or 0)/energy if energy else None))

    bins, dates, clock = defaultdict(list), defaultdict(list), defaultdict(list)
    for h in hours:
        bins[interval.floor(h)].append(h)
        dates[h.astimezone(ATHENS).date()].append(h)
        clock[h.astimezone(ATHENS).hour].append(h)
    points = []
    for _, selected in sorted(bins.items()):
        energy = total(selected)
        c = coverage(selected)
        points.append(dict(start=instant(selected[0]), end=instant(selected[-1]+HOUR),
            energy_kwh=energy, equivalent_co2_kg=energy*factor if energy is not None else None,
            outside_school_hours_kwh=school_summary(selected)['outside_school_hours_kwh'],
            observed_series_hours=c['observed_series_hours'], expected_series_hours=c['expected_series_hours'],
            quality=c['quality'], load_energy_kwh=[total(selected, m) for m in load_members],
            load_equivalent_co2_kg=[e*factor if e is not None else None for e in (total(selected, m) for m in load_members)]))
    breakdown = []
    for load, members in zip(loads, load_members):
        source = sources[members[0]]
        observed = sum(i in values[h] for h in hours for i in members)
        expected = len(hours)*len(members)
        breakdown.append(dict(id=load, label=source.label, room_id=source.room_id,
            phases=[sources[i].phase for i in members if sources[i].phase],
            energy_kwh=total(hours, members), observed_series_hours=observed,
            expected_series_hours=expected, quality=quality(observed, expected)))
    observed_totals = [total([h]) for h in hours if values[h]]
    energy = total(hours)
    cov = coverage(hours)
    # Normalize the mean hourly profile, so missing dates do not overweight hours.
    means = {h: total(hs)/sum(bool(values[t]) for t in hs)
             for h, hs in clock.items() if any(values[t] for t in hs)}
    profile_total = fsum(means.values())
    return ConsumptionHistory(school_id=school_id, room_id=room_id, source_basis=basis,
        measurement_scope=scope, interval=interval.value, start=instant(start), end=instant(end),
        fetched_at=instant(datetime.now(UTC)), **emission, points=points, coverage=cov, breakdown=breakdown,
        summary=dict(total_energy_kwh=energy, equivalent_co2_kg=energy*factor if energy is not None else None,
            mean_hourly_energy_kwh=energy/len(observed_totals) if observed_totals else None,
            peak_hourly_energy_kwh=max(observed_totals, default=None),
            maximum_interval_energy_kwh=max((p['energy_kwh'] for p in points if p['energy_kwh'] is not None), default=None),
            measured_hour_count=len(observed_totals), **school_summary(hours)),
        days=[dict(date=str(d), energy_kwh=total(hs), coverage=coverage(hs), is_weekend=d.weekday() >= 5,
                   **school_summary(hs)) for d, hs in sorted(dates.items())],
        hourly_profile=[dict(hour=h, mean_energy_kwh=means.get(h),
            share_pct=100*means[h]/profile_total if h in means and profile_total else None,
            observed_hours=sum(bool(values[t]) for t in clock[h])) for h in range(24)])


class ConsumptionCache:
    """Bounded, short-lived prepared results, coalescing identical concurrent reads.

    Authorization and catalog resolution happen before lookup. Catalog topology,
    emissions metadata, scope and bounds all participate in the key.
    """
    def __init__(self, capacity=8, ttl=60):
        self.capacity, self.ttl = capacity, ttl
        self.lock, self.entries, self.pending = Lock(), OrderedDict(), {}

    def get(self, key, build):
        with self.lock:
            cached = self.entries.get(key)
            if cached and cached[0] > monotonic():
                self.entries.move_to_end(key)
                return cached[1]
            existing = self.pending.get(key)
            if existing is None:
                future = self.pending[key] = Future()
        if existing is not None:
            return existing.result()
        try:
            result = build()
            with self.lock:
                self.entries[key] = (monotonic()+self.ttl, result)
                self.entries.move_to_end(key)
                while len(self.entries) > self.capacity:
                    self.entries.popitem(last=False)
            future.set_result(result)
            return result
        except BaseException as exc:
            future.set_exception(exc)
            raise
        finally:
            with self.lock:
                self.pending.pop(key, None)


cache = ConsumptionCache()


def get_consumption(school_id, room_id, start, end, interval):
    sources, basis, scope = source_plan(school_id, room_id)
    parsed = consumption_interval(interval)
    key = (school_id, room_id, sources, basis, scope, start, end, parsed.value,
           tuple(_emissions_factor_contract().items()))
    return cache.get(key, lambda: prepare(sources, basis, scope, school_id, room_id, start, end, parsed))
