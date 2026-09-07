"""Deterministic, meter-aware school energy insights.

The MVP consumes the existing precomputed Shelly hourly-energy resource. It
keeps missing buckets distinct from observed zeroes, resolves one authoritative
school series from the meter topology, and performs bounded, explainable
analysis over recent hourly and longer daily features.
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from statistics import median
from zoneinfo import ZoneInfo

from monitoring.config import (
    ENERGY_CO2_FACTOR_KG_PER_KWH,
    ENERGY_CO2_FACTOR_REFERENCE_YEAR,
    ENERGY_CO2_FACTOR_SOURCE,
    ENERGY_CO2_FACTOR_VERSION,
)
from monitoring.policies.school_hours import is_local_school_hour
from monitoring.policies.school_hours import is_school_hour
from monitoring.schemas import (
    EnergyHourlyBaselineBucket,
    EnergyInsightCard,
    EnergyInsightEvidence,
    SchoolEnergyInsightsResponse,
)
from monitoring.services.energy_demand.devices import list_energy_devices_for_school
from monitoring.services.insights.algorithms import average_linkage_clusters
from monitoring.services.insights.algorithms import contiguous_groups
from monitoring.services.insights.algorithms import cyclic_distance as _cyclic_distance
from monitoring.services.insights.algorithms import optimal_change_points as _optimal_change_points
from monitoring.services.insights.algorithms import percentile as _percentile
from monitoring.services.service_energy_api import fetch_shelly_hourly_energy
from monitoring.utils.timezone import APP_TIMEZONE, as_utc

logger = logging.getLogger(__name__)

POLICY_VERSION = "energy-insights-v1.4"
LOOKBACK_DAYS = 90
SHORT_TERM_DAYS = 14
FRESHNESS_HOURS = 6
MIN_DEVICE_BUCKETS = 72
MIN_DEVICE_RECENT_COVERAGE = 0.70
MIN_AGGREGATE_DEVICE_COVERAGE = 0.80
MIN_ATTRIBUTION_COMMON_COVERAGE = 0.60
ATTRIBUTION_ZERO_THRESHOLD_WH = 1.0
MIN_DAILY_COVERAGE = 0.75
MIN_STABILITY_COVERAGE = 0.85
MAX_CLUSTER_EPISODES = 120

_UTC = timezone.utc


@dataclass(frozen=True)
class _HourlyPoint:
    stamp: datetime
    local_stamp: datetime
    value_wh: float
    source_coverage: float = 1.0
    source_values_wh: tuple[tuple[str, float], ...] = ()


@dataclass(frozen=True)
class _EnrichedHour:
    point: _HourlyPoint
    baseline_wh: float
    residual_wh: float
    threshold_wh: float
    significant: bool
    school_hour: bool


@dataclass(frozen=True)
class _HourlyReference:
    baseline_by_slot: dict[tuple[int, int], float]
    mad_by_slot: dict[tuple[int, int], float]
    school_p95: float


@dataclass
class _DailyFeature:
    day: date
    total_wh: float
    baseline_wh: float
    excess_wh: float
    after_hours_excess_wh: float
    peak_wh: float
    closed_mean_wh: float
    active_hours: int
    coverage: float
    regime: str = "single_regime"


@dataclass(frozen=True)
class _Episode:
    id: str
    start: datetime
    end: datetime
    excess_wh: float
    peak_wh: float
    duration_hours: int
    hour_phase: float
    weekday_phase: float
    after_hours_share: float
    magnitude: float
    burden: float
    persistence: float


def _school_timezone() -> ZoneInfo:
    return APP_TIMEZONE


def _completed_window_utc(
    *, now: datetime | None = None
) -> tuple[datetime, datetime]:
    resolved = now or datetime.now(_UTC)
    end = as_utc(resolved).replace(minute=0, second=0, microsecond=0)
    return end - timedelta(days=LOOKBACK_DAYS), end


def _parse_datetime(value) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def _energy_total_wh(item: dict) -> float | None:
    energy = item.get("energy_wh")
    if not isinstance(energy, dict):
        return None
    raw_total = energy.get("total")
    if isinstance(raw_total, (int, float)):
        total = float(raw_total)
    else:
        phases = [energy.get(key) for key in ("a", "b", "c")]
        valid_phases = [float(value) for value in phases if isinstance(value, (int, float))]
        if not valid_phases:
            return None
        total = sum(valid_phases)
    if not math.isfinite(total) or total < 0:
        return None
    return total


def _mad(values: list[float], *, center: float | None = None) -> float:
    if not values:
        return 0.0
    resolved_center = median(values) if center is None else center
    return median(abs(value - resolved_center) for value in values)


def _device_label(device: dict) -> str:
    for key in ("label", "room_alias", "room_id", "id"):
        value = device.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "Unknown meter"


def _scope_labels(devices: list[dict], *, whole_building: bool) -> list[str]:
    if whole_building:
        return ["Whole building"]
    labels = {
        str(device.get("room_alias") or device.get("label") or device.get("room_id") or "Metered load")
        for device in devices
    }
    return sorted(labels)


def _attributed_scope_labels(
    hours: list[_EnrichedHour],
    *,
    source_labels: dict[str, str] | None,
    fallback: list[str],
) -> list[str]:
    """Return only meters with a material reading in the finding's evidence hours.

    The raw readings remain unchanged for analysis. Attribution alone treats an
    hourly reading of 0.001 kWh (1 Wh) or less as effectively zero.
    """
    if not source_labels:
        return fallback
    contributor_ids = {
        device_id
        for hour in hours
        for device_id, value_wh in hour.point.source_values_wh
        if value_wh > ATTRIBUTION_ZERO_THRESHOLD_WH
    }
    return sorted(
        {
            source_labels[device_id]
            for device_id in contributor_ids
            if device_id in source_labels
        }
    )


def _normalize_history(
    payload: dict,
    *,
    device_ids: set[str],
    start: datetime,
    end: datetime,
) -> tuple[dict[str, dict[datetime, float]], datetime | None, int, int]:
    """Validate and deduplicate hourly rows, retaining the newest created row."""
    rows: dict[str, dict[datetime, tuple[datetime, float]]] = {
        device_id: {} for device_id in device_ids
    }
    data_through: datetime | None = None
    invalid_count = 0
    duplicate_count = 0
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        return ({device_id: {} for device_id in device_ids}, None, 0, 0)

    for item in items:
        if not isinstance(item, dict):
            invalid_count += 1
            continue
        device_id = item.get("device_id")
        stamp = _parse_datetime(item.get("window_start"))
        window_end = _parse_datetime(item.get("window_end"))
        value_wh = _energy_total_wh(item)
        if (
            device_id not in device_ids
            or stamp is None
            or value_wh is None
            or stamp < start
            or stamp >= end
        ):
            invalid_count += 1
            continue
        created_at = _parse_datetime(item.get("created_at")) or stamp
        current = rows[device_id].get(stamp)
        if current is not None:
            duplicate_count += 1
            if created_at <= current[0]:
                continue
        rows[device_id][stamp] = (created_at, value_wh)
        resolved_end = window_end or stamp + timedelta(hours=1)
        if data_through is None or resolved_end > data_through:
            data_through = resolved_end

    normalized = {
        device_id: {stamp: value for stamp, (_, value) in device_rows.items()}
        for device_id, device_rows in rows.items()
    }
    return normalized, data_through, invalid_count, duplicate_count


def _recent_device_quality(
    rows: dict[datetime, float],
    *,
    latest_stamp: datetime,
    range_end: datetime,
) -> tuple[bool, float, str | None]:
    recent_start = range_end - timedelta(days=SHORT_TERM_DAYS)
    recent_count = sum(recent_start <= stamp < range_end for stamp in rows)
    coverage = min(1.0, recent_count / (SHORT_TERM_DAYS * 24))
    if not rows:
        return False, coverage, "no hourly data"
    device_latest = max(rows)
    if device_latest < latest_stamp - timedelta(hours=FRESHNESS_HOURS):
        return (
            False,
            coverage,
            f"offline/stale; last reported {device_latest.date().isoformat()}",
        )
    if recent_count < MIN_DEVICE_BUCKETS:
        return False, coverage, f"only {recent_count} recent hourly buckets"
    if coverage < MIN_DEVICE_RECENT_COVERAGE:
        return False, coverage, f"{coverage * 100:.0f}% recent coverage"
    return True, coverage, None


def _resolve_school_series(
    devices: list[dict],
    rows_by_device: dict[str, dict[datetime, float]],
    *,
    range_start: datetime,
    range_end: datetime,
) -> tuple[str, list[_HourlyPoint], list[dict], list[str], bool]:
    warnings: list[str] = []
    whole_building = [
        device
        for device in devices
        if device.get("type") == "three_phase_meter"
        and str(device.get("room_id") or "").strip() == "all_rooms"
    ]
    if len(whole_building) > 1:
        return (
            "unavailable",
            [],
            [],
            ["Multiple whole-building meters are configured; meter topology must be clarified."],
            True,
        )

    latest_candidates = [max(rows) for rows in rows_by_device.values() if rows]
    if not latest_candidates:
        return "unavailable", [], [], ["No valid hourly energy data is available."], True
    latest_stamp = max(latest_candidates)

    candidates = whole_building if whole_building else devices
    included: list[dict] = []
    for device in candidates:
        device_id = str(device.get("id") or "")
        include, _, reason = _recent_device_quality(
            rows_by_device.get(device_id, {}),
            latest_stamp=latest_stamp,
            range_end=range_end,
        )
        if include:
            included.append(device)
        else:
            warnings.append(f"{_device_label(device)} excluded: {reason}.")

    if whole_building:
        if not included:
            return "unavailable", [], [], warnings, True
        device = included[0]
        device_rows = rows_by_device.get(str(device.get("id")), {})
        points = [
            _HourlyPoint(
                stamp=stamp,
                local_stamp=stamp.astimezone(_school_timezone()),
                value_wh=value,
                source_values_wh=((str(device.get("id")), value),),
            )
            for stamp, value in sorted(device_rows.items())
            if range_start <= stamp < range_end
        ]
        return "whole_building", points, included, warnings, bool(warnings)

    warnings.insert(
        0,
        "Whole-building meter unavailable; insights cover monitored loads only.",
    )
    if not included:
        return "unavailable", [], [], warnings, True

    required_sources = max(1, math.ceil(len(included) * MIN_AGGREGATE_DEVICE_COVERAGE))
    included_ids = [str(device.get("id")) for device in included]
    stamps = sorted(
        {
            stamp
            for device_id in included_ids
            for stamp in rows_by_device.get(device_id, {})
            if range_start <= stamp < range_end
        }
    )
    points: list[_HourlyPoint] = []
    tz = _school_timezone()
    for stamp in stamps:
        observed = [
            (device_id, rows_by_device[device_id][stamp])
            for device_id in included_ids
            if stamp in rows_by_device.get(device_id, {})
        ]
        if len(observed) < required_sources:
            continue
        points.append(
            _HourlyPoint(
                stamp=stamp,
                local_stamp=stamp.astimezone(tz),
                value_wh=sum(value for _, value in observed),
                source_coverage=len(observed) / len(included_ids),
                source_values_wh=tuple(observed),
            )
        )
    return "metered_loads", points, included, warnings, True


def _learn_hourly_reference(points: list[_HourlyPoint]) -> _HourlyReference:
    if not points:
        return _HourlyReference(
            baseline_by_slot={},
            mad_by_slot={},
            school_p95=0.0,
        )
    by_slot: dict[tuple[int, int], list[float]] = {}
    by_category: dict[bool, list[float]] = {True: [], False: []}
    all_values = [point.value_wh for point in points]
    school_p95 = _percentile(all_values, 0.95)
    for point in points:
        slot = (point.local_stamp.weekday(), point.local_stamp.hour)
        by_slot.setdefault(slot, []).append(point.value_wh)
        by_category[is_local_school_hour(point.local_stamp)].append(point.value_wh)

    category_baseline = {
        category: _percentile(values or all_values, 0.20)
        for category, values in by_category.items()
    }
    category_mad = {
        category: _mad(values or all_values)
        for category, values in by_category.items()
    }
    baseline_by_slot: dict[tuple[int, int], float] = {}
    mad_by_slot: dict[tuple[int, int], float] = {}
    for weekday in range(7):
        for hour in range(24):
            slot = (weekday, hour)
            school_hour = is_school_hour(weekday=weekday, hour=hour)
            slot_values = by_slot.get(slot, [])
            baseline_by_slot[slot] = (
                _percentile(slot_values, 0.20)
                if len(slot_values) >= 3
                else category_baseline[school_hour]
            )
            mad_by_slot[slot] = (
                _mad(slot_values)
                if len(slot_values) >= 3
                else category_mad[school_hour]
            )
    return _HourlyReference(
        baseline_by_slot=baseline_by_slot,
        mad_by_slot=mad_by_slot,
        school_p95=school_p95,
    )


def _enrich_hours(
    points: list[_HourlyPoint],
    *,
    reference: _HourlyReference | None = None,
) -> list[_EnrichedHour]:
    if not points:
        return []
    resolved_reference = reference or _learn_hourly_reference(points)
    enriched: list[_EnrichedHour] = []
    for point in points:
        school_hour = is_local_school_hour(point.local_stamp)
        slot = (point.local_stamp.weekday(), point.local_stamp.hour)
        baseline = resolved_reference.baseline_by_slot[slot]
        slot_mad = resolved_reference.mad_by_slot[slot]
        threshold = max(
            3.0 * 1.4826 * slot_mad,
            0.05 * resolved_reference.school_p95,
            1.0,
        )
        residual = max(0.0, point.value_wh - baseline)
        enriched.append(
            _EnrichedHour(
                point=point,
                baseline_wh=baseline,
                residual_wh=residual,
                threshold_wh=threshold,
                significant=residual > threshold,
                school_hour=school_hour,
            )
        )
    return enriched


def _expected_hours_for_local_day(day: date) -> int:
    tz = _school_timezone()
    start_local = datetime.combine(day, datetime_time.min, tzinfo=tz)
    end_local = datetime.combine(day + timedelta(days=1), datetime_time.min, tzinfo=tz)
    return round(
        (end_local.astimezone(_UTC) - start_local.astimezone(_UTC)).total_seconds()
        / 3600
    )


def _daily_features(
    hours: list[_EnrichedHour],
    *,
    data_through: datetime,
) -> list[_DailyFeature]:
    by_day: dict[date, list[_EnrichedHour]] = {}
    final_partial_day = data_through.astimezone(_school_timezone()).date()
    for hour in hours:
        day = hour.point.local_stamp.date()
        if day >= final_partial_day:
            continue
        by_day.setdefault(day, []).append(hour)

    result: list[_DailyFeature] = []
    for day, day_hours in sorted(by_day.items()):
        expected = _expected_hours_for_local_day(day)
        coverage = sum(hour.point.source_coverage for hour in day_hours) / expected
        if coverage < MIN_DAILY_COVERAGE:
            continue
        closed_values = [hour.point.value_wh for hour in day_hours if not hour.school_hour]
        result.append(
            _DailyFeature(
                day=day,
                total_wh=sum(hour.point.value_wh for hour in day_hours),
                baseline_wh=sum(hour.baseline_wh for hour in day_hours),
                excess_wh=sum(hour.residual_wh for hour in day_hours),
                after_hours_excess_wh=sum(
                    hour.residual_wh for hour in day_hours if not hour.school_hour
                ),
                peak_wh=max(hour.point.value_wh for hour in day_hours),
                closed_mean_wh=(sum(closed_values) / len(closed_values)) if closed_values else 0.0,
                active_hours=sum(hour.significant for hour in day_hours),
                coverage=min(1.0, coverage),
            )
        )
    _infer_daily_regimes(result)
    return result


def _infer_daily_regimes(features: list[_DailyFeature]) -> None:
    """Deterministic two-centroid segmentation of daily excess energy."""
    if len(features) < 10:
        return
    values = [math.log1p(feature.excess_wh / 1000.0) for feature in features]
    low_center = min(values)
    high_center = max(values)
    if high_center - low_center <= 1e-9:
        return

    assignments = [0] * len(values)
    for _ in range(50):
        updated = [
            0 if abs(value - low_center) <= abs(value - high_center) else 1
            for value in values
        ]
        low_values = [value for value, group in zip(values, updated) if group == 0]
        high_values = [value for value, group in zip(values, updated) if group == 1]
        if not low_values or not high_values:
            return
        next_low = sum(low_values) / len(low_values)
        next_high = sum(high_values) / len(high_values)
        if updated == assignments and abs(next_low - low_center) < 1e-9 and abs(next_high - high_center) < 1e-9:
            assignments = updated
            break
        assignments = updated
        low_center, high_center = next_low, next_high

    if sum(group == 0 for group in assignments) < 5 or sum(group == 1 for group in assignments) < 5:
        return
    separation = abs(high_center - low_center)
    if separation < max(0.35, 1.5 * _mad(values)):
        return
    low_group = 0 if low_center < high_center else 1
    for feature, group in zip(features, assignments):
        feature.regime = "low_activity" if group == low_group else "active"


def _group_contiguous_hours(hours: list[_EnrichedHour]) -> list[list[_EnrichedHour]]:
    return contiguous_groups(
        hours,
        time_key=lambda item: item.point.stamp,
        max_gap=timedelta(hours=1),
    )


def _episodes(hours: list[_EnrichedHour], *, horizon_total_wh: float) -> list[_Episode]:
    significant = [hour for hour in hours if hour.significant]
    episodes: list[_Episode] = []
    for index, group in enumerate(_group_contiguous_hours(significant)):
        excess = sum(hour.residual_wh for hour in group)
        peak = max(hour.point.value_wh for hour in group)
        start = group[0].point.local_stamp
        after_hours = sum(not hour.school_hour for hour in group) / len(group)
        episodes.append(
            _Episode(
                id=f"episode-{index}-{group[0].point.stamp.isoformat()}",
                start=group[0].point.stamp,
                end=group[-1].point.stamp,
                excess_wh=excess,
                peak_wh=peak,
                duration_hours=len(group),
                hour_phase=start.hour / 24.0,
                weekday_phase=start.weekday() / 7.0,
                after_hours_share=after_hours,
                magnitude=min(1.0, peak / max(_percentile([item.point.value_wh for item in hours], 0.95), 1.0)),
                burden=min(1.0, excess / max(0.10 * horizon_total_wh, 500.0)),
                persistence=min(1.0, len(group) / 6.0),
            )
        )
    return episodes


def _episode_distance(left: _Episode, right: _Episode) -> float:
    return (
        0.25 * abs(left.magnitude - right.magnitude)
        + 0.20 * abs(left.burden - right.burden)
        + 0.20 * abs(left.persistence - right.persistence)
        + 0.20 * _cyclic_distance(left.hour_phase, right.hour_phase)
        + 0.15 * _cyclic_distance(left.weekday_phase, right.weekday_phase)
    )


def _agglomerative_episode_clusters(
    episodes: list[_Episode],
    *,
    distance_threshold: float = 0.30,
) -> list[list[_Episode]]:
    return average_linkage_clusters(
        episodes,
        distance=_episode_distance,
        sort_key=lambda item: (item.start, item.id),
        distance_threshold=distance_threshold,
        max_items=MAX_CLUSTER_EPISODES,
    )


def _coverage(hours: list[_EnrichedHour], *, expected_hours: int) -> float:
    if expected_hours <= 0:
        return 0.0
    return min(1.0, sum(hour.point.source_coverage for hour in hours) / expected_hours)


def _candidate_score(
    *,
    impact: float,
    persistence: float,
    recurrence: float,
    recency: float,
    confidence: float,
) -> int:
    raw = (
        0.40 * min(1.0, max(0.0, impact))
        + 0.25 * min(1.0, max(0.0, persistence))
        + 0.20 * min(1.0, max(0.0, recurrence))
        + 0.15 * min(1.0, max(0.0, recency))
    )
    return round(100 * raw * min(1.0, max(0.0, confidence)))


def _severity(score: int, *, metering_scope: str, confidence: float) -> str:
    if score < 35:
        return "low"
    if score < 55:
        return "medium"
    if score < 75:
        return "high"
    if metering_scope == "whole_building" and confidence >= 0.90:
        return "critical"
    return "high"


def _format_date(value: date | datetime) -> str:
    day = value.date() if isinstance(value, datetime) else value
    return day.strftime("%d %b %Y").lstrip("0")


def _format_emissions_kg(value: float) -> str:
    if value <= 0:
        return "0.00 kg CO2e"
    if value < 0.01:
        return "<0.01 kg CO2e"
    return f"{value:.2f} kg CO2e"


def _carbon_impact_card(
    hours: list[_EnrichedHour],
    *,
    scope_labels: list[str],
    source_labels: dict[str, str] | None,
    metering_scope: str,
    confidence: float,
) -> EnergyInsightCard | None:
    """Always connect reliable recent electricity use with estimated CO2e."""
    if not hours:
        return None

    total_wh = sum(hour.point.value_wh for hour in hours)
    significant_hours = [hour for hour in hours if hour.significant]
    excess_wh = sum(hour.residual_wh for hour in significant_hours)
    material_excess = (
        len(significant_hours) >= 3
        and excess_wh >= max(500.0, 0.05 * total_wh)
    )
    evidence_hours = significant_hours if material_excess else hours
    attributed_labels = _attributed_scope_labels(
        evidence_hours,
        source_labels=source_labels,
        fallback=scope_labels,
    )

    total_kwh = total_wh / 1000.0
    total_emissions_kg = total_kwh * ENERGY_CO2_FACTOR_KG_PER_KWH
    excess_emissions_kg = (
        excess_wh / 1000.0 * ENERGY_CO2_FACTOR_KG_PER_KWH
    )
    scope_phrase = (
        "the whole building"
        if metering_scope == "whole_building"
        else "the monitored loads"
    )
    factor_label = f"{ENERGY_CO2_FACTOR_KG_PER_KWH:.3f} kg CO2e/kWh"
    above_baseline_share_evidence = (
        EnergyInsightEvidence(
            label="Above-baseline emissions share",
            value=f"{100.0 * excess_wh / total_wh:.1f}%",
        )
        if material_excess and total_wh > 0
        else None
    )

    if material_excess:
        groups = _group_contiguous_hours(significant_hours)
        latest = max(hour.point.stamp for hour in significant_hours)
        age_days = max(
            0.0,
            (hours[-1].point.stamp - latest).total_seconds() / 86400,
        )
        score = _candidate_score(
            impact=min(1.0, excess_wh / max(0.20 * total_wh, 500.0)),
            persistence=min(1.0, len(significant_hours) / 12.0),
            recurrence=min(1.0, len(groups) / 5.0),
            recency=math.exp(-age_days / 14.0),
            confidence=confidence,
        )
        severity = _severity(
            score,
            metering_scope=metering_scope,
            confidence=confidence,
        )
        title = "Above-baseline electricity increased estimated emissions"
        summary = (
            f"Electricity across {scope_phrase} is estimated using the project grid "
            f"factor of {factor_label}; {excess_wh / 1000.0:.1f} kWh above the learned "
            f"baseline corresponds to {_format_emissions_kg(excess_emissions_kg)}."
        )
        index_label = "Criticality index"
        recommendation = (
            "Prioritize the cited loads and verify their operating schedules; reducing "
            "above-baseline electricity would proportionally reduce estimated emissions."
        )
        third_evidence = EnergyInsightEvidence(
            label="Above-baseline emissions",
            value=_format_emissions_kg(excess_emissions_kg),
        )
    else:
        within_share = 1.0 - (
            len(significant_hours) / max(1, len(hours))
        )
        score = round(100 * (0.60 * confidence + 0.40 * within_share))
        severity = "positive"
        title = "Electricity-related emissions for the recent period"
        if total_wh > 0:
            summary = (
                f"Observed electricity across {scope_phrase} corresponds to estimated "
                f"emissions using the project grid factor of {factor_label}; no material "
                "above-baseline carbon-impact finding was detected."
            )
        else:
            summary = (
                f"Valid hourly readings across {scope_phrase} contained no material "
                f"electricity use, resulting in {_format_emissions_kg(total_emissions_kg)} "
                "estimated electricity-related emissions."
            )
        index_label = "Stability index"
        recommendation = (
            "Maintain monitoring and compare the next complete period using the same "
            "documented grid factor."
        )
        third_evidence = EnergyInsightEvidence(
            label="Grid factor",
            value=factor_label,
        )

    period_start = min(hour.point.local_stamp for hour in hours)
    period_end = max(hour.point.local_stamp for hour in hours)
    return EnergyInsightCard(
        id="short-carbon-impact",
        horizon="short_term",
        kind="carbon_impact",
        severity=severity,
        title=title,
        summary=summary,
        period_label=(
            f"{_format_date(period_start)}–{_format_date(period_end)} · hourly derived"
        ),
        scope_labels=attributed_labels,
        index_label=index_label,
        index_score=max(0, min(100, score)),
        evidence=[
            EnergyInsightEvidence(label="Electricity", value=f"{total_kwh:.1f} kWh"),
            EnergyInsightEvidence(
                label="Estimated emissions",
                value=_format_emissions_kg(total_emissions_kg),
            ),
            third_evidence,
            *(
                [above_baseline_share_evidence]
                if above_baseline_share_evidence is not None
                else []
            ),
        ],
        recommendation=recommendation,
    )


def _after_hours_candidate(
    hours: list[_EnrichedHour],
    *,
    daily: list[_DailyFeature],
    scope_labels: list[str],
    source_labels: dict[str, str] | None = None,
    metering_scope: str,
    confidence: float,
) -> EnergyInsightCard | None:
    low_activity_days = {feature.day for feature in daily if feature.regime == "low_activity"}
    affected = [
        hour
        for hour in hours
        if hour.significant
        and (
            not hour.school_hour
            or hour.point.local_stamp.date() in low_activity_days
        )
    ]
    total_wh = sum(hour.point.value_wh for hour in hours)
    excess_wh = sum(hour.residual_wh for hour in affected)
    threshold_wh = max(500.0, 0.05 * total_wh)
    if len(affected) < 3 or excess_wh < threshold_wh:
        return None
    groups = _group_contiguous_hours(affected)
    latest = max(hour.point.stamp for hour in affected)
    age_days = max(0.0, (hours[-1].point.stamp - latest).total_seconds() / 86400)
    score = _candidate_score(
        impact=min(1.0, excess_wh / max(0.20 * total_wh, 500.0)),
        persistence=min(1.0, len(affected) / 12.0),
        recurrence=min(1.0, len(groups) / 5.0),
        recency=math.exp(-age_days / 14.0),
        confidence=confidence,
    )
    start = min(hour.point.local_stamp for hour in affected)
    end = max(hour.point.local_stamp for hour in affected)
    attributed_labels = _attributed_scope_labels(
        affected,
        source_labels=source_labels,
        fallback=scope_labels,
    )
    return EnergyInsightCard(
        id="short-after-hours",
        horizon="short_term",
        kind="after_hours",
        severity=_severity(score, metering_scope=metering_scope, confidence=confidence),
        title="Energy above baseline outside regular hours",
        summary=(
            "Repeated hourly demand rose above the learned inactive baseline during "
            "out-of-hours or low-activity periods."
        ),
        period_label=f"{_format_date(start)}–{_format_date(end)} · hourly",
        scope_labels=attributed_labels,
        index_label="Criticality index",
        index_score=score,
        evidence=[
            EnergyInsightEvidence(label="Excess", value=f"{excess_wh / 1000:.1f} kWh"),
            EnergyInsightEvidence(label="Affected hours", value=str(len(affected))),
            EnergyInsightEvidence(label="Episodes", value=str(len(groups))),
        ],
        recommendation=(
            "Review timers and equipment schedules for the cited periods, then compare the "
            "next readings with the learned inactive baseline."
        ),
    )


def _peak_candidate(
    hours: list[_EnrichedHour],
    *,
    horizon: str,
    scope_labels: list[str],
    source_labels: dict[str, str] | None = None,
    metering_scope: str,
    confidence: float,
    skip_after_hours_cluster: bool,
) -> EnergyInsightCard | None:
    if not hours:
        return None
    total_wh = sum(hour.point.value_wh for hour in hours)
    clusters = _agglomerative_episode_clusters(
        _episodes(hours, horizon_total_wh=total_wh)
    )
    minimum_size = 3 if horizon == "short_term" else 5
    candidates: list[tuple[float, list[_Episode]]] = []
    for cluster in clusters:
        if len(cluster) < minimum_size:
            continue
        cluster_excess = sum(episode.excess_wh for episode in cluster)
        if cluster_excess < max(500.0, 0.03 * total_wh):
            continue
        after_share = sum(episode.after_hours_share for episode in cluster) / len(cluster)
        if skip_after_hours_cluster and after_share >= 0.70:
            continue
        candidates.append((cluster_excess, cluster))
    if not candidates:
        return None
    excess_wh, cluster = max(
        candidates,
        key=lambda item: (item[0], len(item[1]), max(ep.end for ep in item[1])),
    )
    latest = max(episode.end for episode in cluster)
    end_stamp = hours[-1].point.stamp
    decay_days = 14.0 if horizon == "short_term" else 45.0
    age_days = max(0.0, (end_stamp - latest).total_seconds() / 86400)
    score = _candidate_score(
        impact=min(1.0, excess_wh / max(0.20 * total_wh, 500.0)),
        persistence=min(1.0, max(ep.duration_hours for ep in cluster) / 6.0),
        recurrence=min(1.0, len(cluster) / (5.0 if horizon == "short_term" else 8.0)),
        recency=math.exp(-age_days / decay_days),
        confidence=confidence,
    )
    local_starts = [episode.start.astimezone(_school_timezone()) for episode in cluster]
    local_ends = [episode.end.astimezone(_school_timezone()) for episode in cluster]
    cluster_hours = [
        hour
        for hour in hours
        if any(
            episode.start <= hour.point.stamp <= episode.end
            for episode in cluster
        )
    ]
    attributed_labels = _attributed_scope_labels(
        cluster_hours,
        source_labels=source_labels,
        fallback=scope_labels,
    )
    median_hour = round(median(start.hour for start in local_starts))
    horizon_label = "Recent" if horizon == "short_term" else "Long-term"
    return EnergyInsightCard(
        id=f"{horizon}-recurring-peak",
        horizon=horizon,
        kind="recurring_peak",
        severity=_severity(score, metering_scope=metering_scope, confidence=confidence),
        title=f"{horizon_label} recurring peak pattern",
        summary=(
            f"Hierarchical clustering linked {len(cluster)} similar demand episodes, "
            f"typically beginning around {median_hour:02d}:00."
        ),
        period_label=(
            f"{_format_date(min(local_starts))}–{_format_date(max(local_ends))} · clustered hourly"
        ),
        scope_labels=attributed_labels,
        index_label="Criticality index",
        index_score=score,
        evidence=[
            EnergyInsightEvidence(label="Recurring episodes", value=str(len(cluster))),
            EnergyInsightEvidence(label="Cluster excess", value=f"{excess_wh / 1000:.1f} kWh"),
            EnergyInsightEvidence(label="Detected by", value="Hierarchical clustering"),
        ],
        recommendation=(
            "Check whether the repeated start time matches intended occupancy or equipment "
            "operation before adjusting controls."
        ),
    )


def _latest_contiguous_daily_group(features: list[_DailyFeature]) -> list[_DailyFeature]:
    groups: list[list[_DailyFeature]] = []
    for feature in sorted(features, key=lambda item: item.day):
        if not groups or (feature.day - groups[-1][-1].day).days > 1:
            groups.append([feature])
        else:
            groups[-1].append(feature)
    return groups[-1] if groups else []


def _baseload_candidate(
    daily: list[_DailyFeature],
    *,
    scope_labels: list[str],
    hours: list[_EnrichedHour] | None = None,
    source_labels: dict[str, str] | None = None,
    metering_scope: str,
    confidence: float,
) -> EnergyInsightCard | None:
    contiguous = _latest_contiguous_daily_group(daily)
    if len(contiguous) < 14:
        return None
    values = [feature.closed_mean_wh for feature in contiguous]
    change_points = _optimal_change_points(values, min_segment=7)
    if not change_points:
        return None
    change = change_points[-1]
    previous_change = change_points[-2] if len(change_points) > 1 else 0
    before = values[previous_change:change]
    after = values[change:]
    if len(before) < 7 or len(after) < 7:
        return None
    before_mean = sum(before) / len(before)
    after_mean = sum(after) / len(after)
    increase = after_mean - before_mean
    if before_mean <= 0:
        return None
    extra_kwh_day = increase * 24.0 / 1000.0
    if (
        increase < 0.20 * before_mean
        or increase < 3.0 * 1.4826 * _mad(before)
        or extra_kwh_day < 0.5
    ):
        return None
    recent_total_kwh = (
        sum(feature.total_wh for feature in contiguous[change:]) / len(after)
    ) / 1000.0
    latest_day = contiguous[-1].day
    score = _candidate_score(
        impact=min(1.0, extra_kwh_day / max(0.20 * recent_total_kwh, 0.5)),
        persistence=min(1.0, len(after) / 14.0),
        recurrence=1.0,
        recency=math.exp(-max(0, (latest_day - contiguous[change].day).days) / 45.0),
        confidence=confidence,
    )
    percent = 100.0 * increase / before_mean
    post_change_days = {feature.day for feature in contiguous[change:]}
    attributed_labels = _attributed_scope_labels(
        [
            hour
            for hour in (hours or [])
            if hour.point.local_stamp.date() in post_change_days
        ],
        source_labels=source_labels,
        fallback=scope_labels,
    )
    return EnergyInsightCard(
        id="long-baseload-shift",
        horizon="long_term",
        kind="baseload",
        severity=_severity(score, metering_scope=metering_scope, confidence=confidence),
        title="Sustained baseload increase detected",
        summary=(
            "A deterministic change point separates the latest out-of-hours baseload from "
            "the preceding stable period."
        ),
        period_label=(
            f"Since {_format_date(contiguous[change].day)} · daily derived"
        ),
        scope_labels=attributed_labels,
        index_label="Criticality index",
        index_score=score,
        evidence=[
            EnergyInsightEvidence(label="Baseline change", value=f"+{percent:.0f}%"),
            EnergyInsightEvidence(label="Estimated uplift", value=f"{extra_kwh_day:.1f} kWh/day"),
            EnergyInsightEvidence(label="Post-change days", value=str(len(after))),
        ],
        recommendation=(
            "Review continuously powered loads and recent control changes; confirm the shift "
            "against the next complete week before taking corrective action."
        ),
    )


def _latest_low_activity_run(daily: list[_DailyFeature]) -> list[_DailyFeature]:
    run: list[_DailyFeature] = []
    for feature in sorted(daily, key=lambda item: item.day):
        if feature.regime != "low_activity":
            run = []
            continue
        if run and (feature.day - run[-1].day).days > 1:
            run = []
        run.append(feature)
    return run


def _recent_increase_candidate(
    daily: list[_DailyFeature],
    *,
    range_end: datetime,
    scope_labels: list[str],
    hours: list[_EnrichedHour] | None = None,
    source_labels: dict[str, str] | None = None,
    metering_scope: str,
    confidence: float,
) -> EnergyInsightCard | None:
    """Compare recent daily energy with same-weekday values from the prior six weeks."""
    local_end_day = range_end.astimezone(_school_timezone()).date()
    recent_start = local_end_day - timedelta(days=SHORT_TERM_DAYS)
    reference_start = recent_start - timedelta(days=42)
    recent = [feature for feature in daily if recent_start <= feature.day < local_end_day]
    reference = [
        feature
        for feature in daily
        if reference_start <= feature.day < recent_start
    ]
    if len(recent) < 7 or len(reference) < 21:
        return None

    reference_by_weekday: dict[int, list[float]] = {}
    for feature in reference:
        reference_by_weekday.setdefault(feature.day.weekday(), []).append(feature.total_wh)
    weekday_medians = {
        weekday: median(values)
        for weekday, values in reference_by_weekday.items()
        if len(values) >= 3
    }
    comparable = [
        (feature, weekday_medians[feature.day.weekday()])
        for feature in recent
        if feature.day.weekday() in weekday_medians
    ]
    if len(comparable) < 7:
        return None

    observed_wh = sum(feature.total_wh for feature, _ in comparable)
    expected_wh = sum(expected for _, expected in comparable)
    if expected_wh <= 0:
        return None
    increase_wh = observed_wh - expected_wh
    increase_pct = increase_wh / expected_wh
    increase_wh_day = increase_wh / len(comparable)

    reference_deviations = [
        feature.total_wh - weekday_medians[feature.day.weekday()]
        for feature in reference
        if feature.day.weekday() in weekday_medians
    ]
    robust_noise_wh = 2.0 * 1.4826 * _mad(reference_deviations, center=0.0)
    if (
        increase_pct < 0.15
        or increase_wh_day < 500.0
        or increase_wh_day < robust_noise_wh
    ):
        return None

    higher_days = sum(
        feature.total_wh > expected for feature, expected in comparable
    )
    score = _candidate_score(
        impact=min(1.0, max(increase_pct / 0.50, increase_wh_day / 3000.0)),
        persistence=higher_days / len(comparable),
        recurrence=min(1.0, len(comparable) / SHORT_TERM_DAYS),
        recency=1.0,
        confidence=confidence,
    )
    severity = _severity(
        score,
        metering_scope=metering_scope,
        confidence=confidence,
    )
    if severity == "critical":
        severity = "high"
    comparable_days = {feature.day for feature, _ in comparable}
    attributed_labels = _attributed_scope_labels(
        [
            hour
            for hour in (hours or [])
            if hour.point.local_stamp.date() in comparable_days
        ],
        source_labels=source_labels,
        fallback=scope_labels,
    )
    return EnergyInsightCard(
        id="short-recent-increase",
        horizon="short_term",
        kind="recent_increase",
        severity=severity,
        title="Recent consumption increased versus comparable weekdays",
        summary=(
            f"Across {len(comparable)} valid recent days, energy was above a same-weekday "
            "baseline learned from the preceding six weeks."
        ),
        period_label=(
            f"{_format_date(comparable[0][0].day)}–"
            f"{_format_date(comparable[-1][0].day)} · daily derived"
        ),
        scope_labels=attributed_labels,
        index_label="Criticality index",
        index_score=score,
        evidence=[
            EnergyInsightEvidence(label="Increase", value=f"+{increase_pct * 100:.0f}%"),
            EnergyInsightEvidence(label="Observed", value=f"{observed_wh / 1000:.1f} kWh"),
            EnergyInsightEvidence(
                label="Comparable baseline",
                value=f"{expected_wh / 1000:.1f} kWh",
            ),
        ],
        recommendation=(
            "Check whether recent occupancy, schedules, or equipment operation explain the "
            "increase before changing controls."
        ),
    )


def _persistent_baseload_candidate(
    hours: list[_EnrichedHour],
    *,
    daily: list[_DailyFeature],
    scope_labels: list[str],
    source_labels: dict[str, str] | None = None,
    metering_scope: str,
    confidence: float,
) -> EnergyInsightCard | None:
    """Describe continuous background demand during an inferred low-activity run."""
    low_activity = _latest_low_activity_run(daily)
    if len(low_activity) < 5:
        return None
    latest_observed_day = max(
        hour.point.local_stamp.date() for hour in hours
    ) if hours else None
    if (
        latest_observed_day is None
        or (latest_observed_day - low_activity[-1].day).days > 1
    ):
        return None
    run_days = {feature.day for feature in low_activity}
    run_hours = [
        hour for hour in hours if hour.point.local_stamp.date() in run_days
    ]
    expected_hours = sum(_expected_hours_for_local_day(day) for day in run_days)
    run_coverage = _coverage(run_hours, expected_hours=expected_hours)
    if run_coverage < MIN_DAILY_COVERAGE:
        return None

    positive_share = sum(hour.point.value_wh > 0 for hour in run_hours) / max(
        1, len(run_hours)
    )
    if positive_share < 0.75:
        return None
    background_wh = sum(
        min(hour.point.value_wh, hour.baseline_wh) for hour in run_hours
    )
    background_wh_day = background_wh / len(low_activity)
    observed_wh = sum(hour.point.value_wh for hour in run_hours)
    background_share = background_wh / observed_wh if observed_wh > 0 else 0.0
    if background_wh_day < 500.0 or background_share < 0.10:
        return None

    resolved_confidence = min(confidence, run_coverage)
    score = _candidate_score(
        impact=min(1.0, background_share / 0.50),
        persistence=min(1.0, len(low_activity) / 30.0),
        recurrence=positive_share,
        recency=1.0,
        confidence=resolved_confidence,
    )
    severity = _severity(
        score,
        metering_scope=metering_scope,
        confidence=resolved_confidence,
    )
    if severity == "critical":
        severity = "high"
    attributed_labels = _attributed_scope_labels(
        run_hours,
        source_labels=source_labels,
        fallback=scope_labels,
    )
    return EnergyInsightCard(
        id="long-persistent-baseload",
        horizon="long_term",
        kind="persistent_baseload",
        severity=severity,
        title="Background demand persisted during low activity",
        summary=(
            "Valid hourly readings show a continuous learned background load across an "
            "inferred low-activity period."
        ),
        period_label=(
            f"{_format_date(low_activity[0].day)}–"
            f"{_format_date(low_activity[-1].day)} · daily derived"
        ),
        scope_labels=attributed_labels,
        index_label="Criticality index",
        index_score=score,
        evidence=[
            EnergyInsightEvidence(
                label="Background demand",
                value=f"{background_wh_day / 1000:.1f} kWh/day",
            ),
            EnergyInsightEvidence(
                label="Low-activity run",
                value=f"{len(low_activity)} days",
            ),
            EnergyInsightEvidence(
                label="Continuous readings",
                value=f"{positive_share * 100:.0f}% of hours",
            ),
        ],
        recommendation=(
            "Review which essential loads should remain continuously powered during "
            "low-activity periods and confirm that their schedules are intentional."
        ),
    )


def _dominant_load_candidate(
    rows_by_device: dict[str, dict[datetime, float]],
    *,
    included: list[dict],
    points: list[_HourlyPoint],
    range_end: datetime,
    confidence: float,
) -> EnergyInsightCard | None:
    """Attribute recent metered-load energy using complete common device hours only."""
    if len(included) < 2:
        return None
    device_ids = [str(device.get("id") or "") for device in included]
    if any(not device_id for device_id in device_ids):
        return None
    recent_start = range_end - timedelta(days=SHORT_TERM_DAYS)
    aggregate_stamps = {
        point.stamp for point in points if recent_start <= point.stamp < range_end
    }
    common_stamps = sorted(
        stamp
        for stamp in aggregate_stamps
        if all(stamp in rows_by_device.get(device_id, {}) for device_id in device_ids)
    )
    common_coverage = len(common_stamps) / (SHORT_TERM_DAYS * 24)
    if common_coverage < MIN_ATTRIBUTION_COMMON_COVERAGE:
        return None

    totals = [
        (
            device,
            sum(rows_by_device[device_id][stamp] for stamp in common_stamps),
        )
        for device, device_id in zip(included, device_ids)
    ]
    total_wh = sum(value for _, value in totals)
    if total_wh < 5000.0:
        return None
    top_device, top_wh = max(
        totals,
        key=lambda item: (item[1], str(item[0].get("id") or "")),
    )
    top_share = top_wh / total_wh
    if top_share < 0.50:
        return None

    daily_totals: dict[date, dict[str, float]] = {}
    tz = _school_timezone()
    for stamp in common_stamps:
        day_values = daily_totals.setdefault(
            stamp.astimezone(tz).date(),
            {device_id: 0.0 for device_id in device_ids},
        )
        for device_id in device_ids:
            day_values[device_id] += rows_by_device[device_id][stamp]
    top_device_id = str(top_device.get("id"))
    dominant_days = sum(
        max(values, key=lambda device_id: (values[device_id], device_id))
        == top_device_id
        for values in daily_totals.values()
    )
    recurrence = dominant_days / max(1, len(daily_totals))
    resolved_confidence = min(confidence, common_coverage)
    score = _candidate_score(
        impact=top_share,
        persistence=common_coverage,
        recurrence=recurrence,
        recency=1.0,
        confidence=resolved_confidence,
    )
    label = _device_label(top_device)
    local_start = common_stamps[0].astimezone(tz)
    local_end = common_stamps[-1].astimezone(tz)
    return EnergyInsightCard(
        id=f"short-dominant-load-{top_device_id}",
        horizon="short_term",
        kind="dominant_load",
        severity=_severity(
            score,
            metering_scope="metered_loads",
            confidence=resolved_confidence,
        ),
        title=f"{label} dominated monitored consumption",
        summary=(
            f"This load accounted for {top_share * 100:.0f}% of energy measured across "
            f"{len(common_stamps)} complete common hours for the included monitored loads."
        ),
        period_label=(
            f"{_format_date(local_start)}–{_format_date(local_end)} · hourly"
        ),
        scope_labels=[label],
        index_label="Criticality index",
        index_score=score,
        evidence=[
            EnergyInsightEvidence(label="Share", value=f"{top_share * 100:.0f}%"),
            EnergyInsightEvidence(label="Energy", value=f"{top_wh / 1000:.1f} kWh"),
            EnergyInsightEvidence(
                label="Common coverage",
                value=f"{common_coverage * 100:.0f}%",
            ),
        ],
        recommendation=(
            "Confirm that this concentration matches intended equipment use and review "
            "the load's operating schedule before making changes."
        ),
    )


def _stability_card(
    hours: list[_EnrichedHour],
    *,
    horizon: str,
    daily: list[_DailyFeature],
    scope_labels: list[str],
    source_labels: dict[str, str] | None = None,
    coverage: float,
) -> EnergyInsightCard:
    within_share = 1.0 - (sum(hour.significant for hour in hours) / max(1, len(hours)))
    score = round(100 * (0.60 * coverage + 0.40 * within_share))
    low_activity = _latest_low_activity_run(daily)
    if len(low_activity) >= 5:
        title = "Stable low-activity energy regime"
        summary = (
            "The latest complete days form a sustained low-activity regime while the meter "
            "continues to report valid hourly data."
        )
        extra_evidence = EnergyInsightEvidence(
            label="Low-activity run", value=f"{len(low_activity)} days"
        )
    else:
        title = "Consumption remained within the learned range"
        summary = (
            "No material after-hours, baseload-shift, or recurring-peak finding outranked "
            "the stability threshold for this horizon."
        )
        extra_evidence = EnergyInsightEvidence(
            label="Within range", value=f"{within_share * 100:.0f}% of hours"
        )
    attributed_labels = _attributed_scope_labels(
        hours,
        source_labels=source_labels,
        fallback=scope_labels,
    )
    period_start = min(hour.point.local_stamp for hour in hours)
    period_end = max(hour.point.local_stamp for hour in hours)
    return EnergyInsightCard(
        id=f"{horizon}-stability",
        horizon=horizon,
        kind="stability",
        severity="positive",
        title=title,
        summary=summary,
        period_label=(
            f"{_format_date(period_start)}–{_format_date(period_end)} · "
            f"{'hourly' if horizon == 'short_term' else 'daily derived'}"
        ),
        scope_labels=attributed_labels,
        index_label="Stability index",
        index_score=max(0, min(100, score)),
        evidence=[
            EnergyInsightEvidence(label="Coverage", value=f"{coverage * 100:.0f}%"),
            extra_evidence,
        ],
        recommendation="Maintain monitoring and review the next generated insight snapshot.",
    )


def _response_without_series(
    school_id: str,
    *,
    total_devices: int,
    warnings: list[str],
    started: float,
) -> SchoolEnergyInsightsResponse:
    return SchoolEnergyInsightsResponse(
        school_id=school_id,
        generated_at=datetime.now(_UTC),
        data_through=None,
        policy_version=POLICY_VERSION,
        emissions_factor_kg_per_kwh=ENERGY_CO2_FACTOR_KG_PER_KWH,
        emissions_factor_source=ENERGY_CO2_FACTOR_SOURCE,
        emissions_factor_reference_year=ENERGY_CO2_FACTOR_REFERENCE_YEAR,
        emissions_factor_version=ENERGY_CO2_FACTOR_VERSION,
        metering_scope="unavailable",
        total_devices=total_devices,
        devices_analyzed=0,
        coverage_pct=0.0,
        insights=[],
        partial=True,
        warnings=warnings,
        computation_ms=round((time.perf_counter() - started) * 1000, 1),
    )


def build_school_energy_insights(
    school_id: str,
    *,
    devices: list[dict] | None = None,
    now: datetime | None = None,
) -> SchoolEnergyInsightsResponse:
    started = time.perf_counter()
    resolved_devices = [
        device
        for device in (devices if devices is not None else list_energy_devices_for_school(school_id))
        if device.get("type") in {"plug", "three_phase_meter"}
    ]
    if not resolved_devices:
        return _response_without_series(
            school_id,
            total_devices=0,
            warnings=["No energy meters are configured for this school."],
            started=started,
        )

    whole_building = [
        device
        for device in resolved_devices
        if device.get("type") == "three_phase_meter"
        and str(device.get("room_id") or "").strip() == "all_rooms"
    ]
    if len(whole_building) > 1:
        return _response_without_series(
            school_id,
            total_devices=len(resolved_devices),
            warnings=["Multiple whole-building meters are configured; meter topology must be clarified."],
            started=started,
        )

    range_start, range_end = _completed_window_utc(now=now)
    device_ids = [str(device.get("id")) for device in resolved_devices if device.get("id")]
    raw = fetch_shelly_hourly_energy(
        device_ids,
        start=range_start,
        end=range_end,
        working_only=False,
    )
    rows, data_through, invalid_count, duplicate_count = _normalize_history(
        raw,
        device_ids=set(device_ids),
        start=range_start,
        end=range_end,
    )
    if data_through is None:
        return _response_without_series(
            school_id,
            total_devices=len(resolved_devices),
            warnings=["No valid hourly energy data is available."],
            started=started,
        )

    metering_scope, points, included, warnings, partial = _resolve_school_series(
        resolved_devices,
        rows,
        range_start=range_start,
        range_end=range_end,
    )
    if invalid_count:
        warnings.append(f"Ignored {invalid_count} invalid hourly row(s).")
    if duplicate_count:
        warnings.append(f"Resolved {duplicate_count} duplicate hourly row(s) using newest created_at.")
    if metering_scope == "unavailable" or not points:
        return SchoolEnergyInsightsResponse(
            school_id=school_id,
            generated_at=datetime.now(_UTC),
            data_through=data_through,
            policy_version=POLICY_VERSION,
            emissions_factor_kg_per_kwh=ENERGY_CO2_FACTOR_KG_PER_KWH,
            emissions_factor_source=ENERGY_CO2_FACTOR_SOURCE,
            emissions_factor_reference_year=ENERGY_CO2_FACTOR_REFERENCE_YEAR,
            emissions_factor_version=ENERGY_CO2_FACTOR_VERSION,
            metering_scope="unavailable",
            total_devices=len(resolved_devices),
            devices_analyzed=0,
            coverage_pct=0.0,
            insights=[],
            partial=True,
            warnings=warnings,
            computation_ms=round((time.perf_counter() - started) * 1000, 1),
        )

    hourly_reference = _learn_hourly_reference(points)
    enriched = _enrich_hours(points, reference=hourly_reference)
    daily = _daily_features(enriched, data_through=data_through)
    expected_long_hours = LOOKBACK_DAYS * 24
    long_coverage = min(
        1.0,
        sum(hour.point.source_coverage for hour in enriched) / expected_long_hours,
    )
    short_start = range_end - timedelta(days=SHORT_TERM_DAYS)
    short_hours = [hour for hour in enriched if hour.point.stamp >= short_start]
    short_coverage = _coverage(short_hours, expected_hours=SHORT_TERM_DAYS * 24)
    scope_labels = _scope_labels(
        included,
        whole_building=metering_scope == "whole_building",
    )
    source_labels = (
        {
            str(device.get("id")): str(
                device.get("room_alias")
                or device.get("label")
                or device.get("room_id")
                or device.get("id")
            )
            for device in included
            if device.get("id")
        }
        if metering_scope == "metered_loads"
        else None
    )
    topology_confidence = 1.0 if metering_scope == "whole_building" else 0.75
    short_confidence = min(short_coverage, topology_confidence)
    long_confidence = min(long_coverage, topology_confidence)

    short_findings: list[EnergyInsightCard] = []
    after_hours = _after_hours_candidate(
        short_hours,
        daily=daily,
        scope_labels=scope_labels,
        source_labels=source_labels,
        metering_scope=metering_scope,
        confidence=short_confidence,
    )
    if after_hours is not None:
        short_findings.append(after_hours)
    short_peak = _peak_candidate(
        short_hours,
        horizon="short_term",
        scope_labels=scope_labels,
        source_labels=source_labels,
        metering_scope=metering_scope,
        confidence=short_confidence,
        skip_after_hours_cluster=after_hours is not None,
    )
    if short_peak is not None:
        short_findings.append(short_peak)
    recent_increase = _recent_increase_candidate(
        daily,
        range_end=range_end,
        scope_labels=scope_labels,
        hours=short_hours,
        source_labels=source_labels,
        metering_scope=metering_scope,
        confidence=short_confidence,
    )
    if recent_increase is not None:
        short_findings.append(recent_increase)
    if metering_scope == "metered_loads":
        dominant_load = _dominant_load_candidate(
            rows,
            included=included,
            points=points,
            range_end=range_end,
            confidence=short_confidence,
        )
        if dominant_load is not None:
            short_findings.append(dominant_load)

    long_candidates: list[EnergyInsightCard] = []
    baseload = _baseload_candidate(
        daily,
        scope_labels=scope_labels,
        hours=enriched,
        source_labels=source_labels,
        metering_scope=metering_scope,
        confidence=long_confidence,
    )
    if baseload is not None:
        long_candidates.append(baseload)
    long_peak = _peak_candidate(
        enriched,
        horizon="long_term",
        scope_labels=scope_labels,
        source_labels=source_labels,
        metering_scope=metering_scope,
        confidence=long_confidence,
        skip_after_hours_cluster=False,
    )
    if long_peak is not None:
        long_candidates.append(long_peak)
    persistent_baseload = _persistent_baseload_candidate(
        enriched,
        daily=daily,
        scope_labels=scope_labels,
        source_labels=source_labels,
        metering_scope=metering_scope,
        confidence=long_confidence,
    )
    if persistent_baseload is not None:
        long_candidates.append(persistent_baseload)

    short_findings = sorted(
        short_findings,
        key=lambda item: (item.index_score, item.id),
        reverse=True,
    )[:2]
    long_candidates = sorted(
        long_candidates,
        key=lambda item: (item.index_score, item.id),
        reverse=True,
    )[:3]
    if not short_findings and short_coverage >= MIN_STABILITY_COVERAGE:
        short_findings.append(
            _stability_card(
                short_hours,
                horizon="short_term",
                daily=daily,
                scope_labels=scope_labels,
                source_labels=source_labels,
                coverage=short_coverage,
            )
        )
    carbon_impact = _carbon_impact_card(
        short_hours,
        scope_labels=scope_labels,
        source_labels=source_labels,
        metering_scope=metering_scope,
        confidence=short_confidence,
    )
    short_candidates = (
        [carbon_impact, *short_findings]
        if carbon_impact is not None
        else short_findings
    )
    if not long_candidates and long_coverage >= MIN_STABILITY_COVERAGE:
        long_candidates.append(
            _stability_card(
                enriched,
                horizon="long_term",
                daily=daily,
                scope_labels=scope_labels,
                source_labels=source_labels,
                coverage=long_coverage,
            )
        )

    return SchoolEnergyInsightsResponse(
        school_id=school_id,
        generated_at=datetime.now(_UTC),
        data_through=data_through,
        policy_version=POLICY_VERSION,
        emissions_factor_kg_per_kwh=ENERGY_CO2_FACTOR_KG_PER_KWH,
        emissions_factor_source=ENERGY_CO2_FACTOR_SOURCE,
        emissions_factor_reference_year=ENERGY_CO2_FACTOR_REFERENCE_YEAR,
        emissions_factor_version=ENERGY_CO2_FACTOR_VERSION,
        metering_scope=metering_scope,
        total_devices=len(resolved_devices),
        devices_analyzed=len(included),
        coverage_pct=round(long_coverage * 100.0, 1),
        hourly_baseline_profile=[
            EnergyHourlyBaselineBucket(
                weekday=weekday,
                hour=hour,
                baseline_wh=round(
                    hourly_reference.baseline_by_slot[(weekday, hour)],
                    4,
                ),
            )
            for weekday in range(7)
            for hour in range(24)
        ],
        insights=[*short_candidates, *long_candidates],
        partial=partial or bool(warnings),
        warnings=warnings,
        computation_ms=round((time.perf_counter() - started) * 1000, 1),
    )


def get_school_energy_insights(school_id: str) -> SchoolEnergyInsightsResponse:
    return build_school_energy_insights(school_id)
