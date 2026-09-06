"""Additive consumption contract; aggregation stays beside the canonical VPS query."""
from datetime import datetime, timedelta

from fastapi import HTTPException

from monitoring.policies.school_hours import is_school_hour_in_school_timezone
from monitoring.utils.timezone import as_utc
from monitoring.services.service_energy_api import _accumulate_hourly_series


def build_compact_consumption(items, *, room_key, device_ids, series_plan,
                              start, end, working_only, emissions):
    start, end = as_utc(start), as_utc(end)
    # Count fully contained UTC hours, including DST days with 23 or 25 hours.
    first = start.replace(minute=0, second=0, microsecond=0)
    if first < start:
        first += timedelta(hours=1)
    expected = set()
    hour = first
    while hour + timedelta(hours=1) <= end:
        if not working_only or is_school_hour_in_school_timezone(hour):
            expected.add(hour)
        hour += timedelta(hours=1)

    bounded = []
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        try:
            ws = as_utc(datetime.fromisoformat(str(item['window_start']).replace('Z', '+00:00')))
            we = as_utc(datetime.fromisoformat(str(item['window_end']).replace('Z', '+00:00')))
        except (ValueError, TypeError, KeyError):
            raise HTTPException(502, "Invalid hourly energy window")
        if ws not in expected:
            continue
        if we - ws != timedelta(hours=1):
            raise HTTPException(502, "Invalid hourly energy duration")
        bounded.append({**item, 'window_start': ws.isoformat().replace('+00:00', 'Z'), 'window_end': we.isoformat().replace('+00:00', 'Z')})

    nested, observed = _accumulate_hourly_series(bounded, series_plan=series_plan, strict=True)
    totals = [0.0] * len(series_plan)
    hour_counts = [0] * len(series_plan)
    points = []
    complete = 0
    for key in sorted(nested):
        indices = observed[key]
        if not indices:
            continue
        values = nested[key]
        points.append(dict(window_start=key[0], window_end=key[1],
                           energy_wh_total=sum(values), observed_series_count=len(indices)))
        complete += len(indices) == len(series_plan)
        for i in indices:
            totals[i] += values[i] / 1000
            hour_counts[i] += 1
    hourly_kwh = [p['energy_wh_total'] / 1000 for p in points]
    total = sum(hourly_kwh) if points else None
    breakdown = [dict(device_id=did if phase is None else f'{did}:phase-{phase}',
                      label=label, phase=phase,
                      energy_kwh=totals[i] if hour_counts[i] else None,
                      observed_hour_count=hour_counts[i])
                 for i, (did, phase, label) in enumerate(series_plan)]
    breakdown.sort(key=lambda x: (x['energy_kwh'] is None, -(x['energy_kwh'] or 0), x['label'], x['device_id']))
    return dict(
        contract='consumption.compact.v1', room_key=room_key,
        start=start.isoformat().replace('+00:00', 'Z'), end=end.isoformat().replace('+00:00', 'Z'), working_only=working_only,
        **emissions, device_ids=device_ids, count=len(points), points=points,
        summary=dict(total_energy_kwh=total,
                     equivalent_co2_kg=total * emissions['emissions_factor_kg_per_kwh'] if total is not None else None,
                     mean_hourly_energy_kwh=total / len(points) if points else None,
                     peak_hourly_energy_kwh=max(hourly_kwh) if points else None,
                     measured_hour_count=len(points)),
        coverage=dict(expected_hour_count=len(expected), observed_hour_count=len(points),
                      complete_hour_count=complete, expected_series_count=len(series_plan)),
        breakdown=breakdown,
    )
