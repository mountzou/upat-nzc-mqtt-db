"""Deterministic, school-level indoor-environment insight generation.

The engine deliberately separates recent hourly episodes from longer-running
daily patterns. Thermal history is first reduced to daily comfort summaries;
daily mean temperature/humidity is never used as a comfort proxy.
"""

from __future__ import annotations

import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Literal

from monitoring.policies.iaq import get_iaq_metric_descriptor, get_iaq_threshold
from monitoring.policies.school_hours import is_local_school_hour
from monitoring.schemas import (
    DeviceHistoryResponse,
    EnvironmentalInsightCard,
    EnvironmentalInsightEvidence,
    SchoolDeviceMetadata,
    SchoolEnvironmentalInsightsResponse,
)
from monitoring.services.environment.devices import list_devices_for_school
from monitoring.services.environment.thermal_comfort import (
    calc_discomfort_index,
    calc_hum_index,
)
from monitoring.services.insights.algorithms import average_linkage_clusters
from monitoring.services.insights.algorithms import contiguous_groups
from monitoring.services.insights.algorithms import cyclic_distance as _cyclic_distance
from monitoring.services.insights.algorithms import optimal_change_points as _optimal_change_points
from monitoring.services.insights.algorithms import percentile as _percentile
from monitoring.services.service_device_api import (
    METRICS_IAQ_DAILY,
    METRICS_THERMAL_COMFORT,
)
from monitoring.services.service_overview import get_device_history

logger = logging.getLogger(__name__)

POLICY_VERSION = "energyplus-environmental-insights-v4"
RECENT_HOURLY_LIMIT = 48
LONG_TERM_DAILY_LIMIT = 365
THERMAL_HOURLY_LIMIT = 14 * 24
MAX_UPSTREAM_CONCURRENCY = 5
REQUIRED_IAQ_KINDS = ("co2", "pm25")
REQUIRED_HORIZONS = ("short_term", "long_term")

_IAQ_POLICY = {
    metric: get_iaq_metric_descriptor(metric)
    for metric in REQUIRED_IAQ_KINDS
}

@dataclass
class _Candidate:
    id: str
    horizon: Literal["short_term", "long_term"]
    kind: Literal["co2", "pm25", "thermal", "overview"]
    severity: Literal["critical", "high", "medium", "low", "positive"]
    title: str
    summary: str
    period_label: str
    room_ids: set[str]
    room_labels: set[str]
    evidence: list[tuple[str, str]]
    recommendation: str
    index_label: Literal["Criticality index", "Stability index"] = "Criticality index"
    index_score: int = 0
    components: dict[str, float] = field(default_factory=dict)
    start: datetime | date | None = None
    end: datetime | date | None = None
    active: bool = False
    burden: float = 0.0
    peak_ratio: float = 0.0

    @property
    def priority(self) -> tuple[int, int, int, int, float, float, str]:
        recurrence = _clamp01(self.components.get("recurrence", 0.0))
        representativeness = _clamp01(
            self.components.get("representativeness", self.components.get("frequency", 0.0))
        )
        confidence = _clamp01(self.components.get("confidence", 1.0))
        selection_score = round(
            0.65 * self.index_score
            + 20 * recurrence
            + 10 * representativeness
            + 5 * confidence
        )
        return (
            selection_score,
            self.index_score,
            int(self.active),
            len(self.room_ids),
            self.burden,
            self.peak_ratio,
            self.id,
        )


@dataclass
class _DeviceSources:
    device: SchoolDeviceMetadata
    recent_iaq: DeviceHistoryResponse | None = None
    daily_iaq: DeviceHistoryResponse | None = None
    thermal_hourly: DeviceHistoryResponse | None = None
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _IntervalResult:
    points: tuple[tuple[datetime | date, float], ...]
    score: float
    signal_count: int
    signal_sum: float


@dataclass(frozen=True)
class _Episode:
    id: str
    horizon: Literal["short_term", "long_term"]
    kind: Literal["co2", "pm25", "thermal"]
    room_id: str
    room_label: str
    start: datetime | date
    end: datetime | date
    values: tuple[float, ...]
    magnitude: float
    burden: float
    persistence: float
    hour_phase: float
    weekday_phase: float


def _reading_value(item, metric: str) -> float | None:
    reading = item.measurements.get(metric)
    value = getattr(reading, "value", None) if reading is not None else None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number


def _format_date(value: datetime | date) -> str:
    date_value = value.date() if isinstance(value, datetime) else value
    return date_value.strftime("%d %b %Y")


def _format_hour(value: datetime) -> str:
    return value.strftime("%d %b, %H:%M")


def _format_period(
    start: datetime | date | None,
    end: datetime | date | None,
    *,
    resolution: str,
) -> str:
    if start is None or end is None:
        return f"{resolution.capitalize()} evidence"
    if isinstance(start, datetime) and isinstance(end, datetime):
        if start.date() == end.date():
            return f"{start.strftime('%d %b %Y')}, {start.strftime('%H:%M')}–{end.strftime('%H:%M')} · {resolution}"
        return f"{_format_hour(start)}–{_format_hour(end)} · {resolution}"
    if start == end:
        return f"{_format_date(start)} · {resolution}"
    return f"{_format_date(start)}–{_format_date(end)} · {resolution}"


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _median(values: list[float]) -> float:
    return _percentile(values, 0.5)


def _criticality_index(
    *,
    magnitude: float,
    frequency: float,
    persistence: float,
    recency: float,
    spread: float,
) -> int:
    """Weighted deterministic risk index; every input is normalized to 0..1."""
    score = 100 * (
        0.30 * _clamp01(magnitude)
        + 0.25 * _clamp01(frequency)
        + 0.20 * _clamp01(persistence)
        + 0.15 * _clamp01(recency)
        + 0.10 * _clamp01(spread)
    )
    return int(round(score))


def _criticality_from_components(components: dict[str, float]) -> int:
    return _criticality_index(
        **{
            name: components.get(name, 0.0)
            for name in ("magnitude", "frequency", "persistence", "recency", "spread")
        }
    )


def _stability_index(
    *,
    compliance: float,
    safety_margin: float,
    improvement: float,
    coverage: float,
) -> int:
    """Positive counterpart: compliance, margin, trend, and evidence coverage."""
    score = 100 * (
        0.45 * _clamp01(compliance)
        + 0.25 * _clamp01(safety_margin)
        + 0.20 * _clamp01(improvement)
        + 0.10 * _clamp01(coverage)
    )
    return int(round(score))


def _severity_from_criticality(index_score: int, *, hard_critical: bool = False) -> str:
    if hard_critical or index_score >= 75:
        return "critical"
    if index_score >= 55:
        return "high"
    if index_score >= 30:
        return "medium"
    return "low"


def _trend_ratio(values: list[tuple[datetime | date, float]], threshold: float) -> float:
    """Robust relative trend using the medians of the first and last thirds."""
    ordered = sorted(values, key=lambda row: row[0])
    if len(ordered) < 4 or threshold <= 0:
        return 0.0
    width = max(1, len(ordered) // 3)
    first = _median([value for _, value in ordered[:width]])
    last = _median([value for _, value in ordered[-width:]])
    return (last - first) / threshold


def _thermal_condition_severity(condition: str) -> int:
    normalized = " ".join(str(condition or "").lower().split())
    rules = (
        ("medical emergency", 4),
        ("heat stroke", 4),
        ("everyone feels severe", 4),
        ("dangerous", 3),
        ("most of the population", 3),
        ("great discomfort", 2),
        ("more than 50%", 2),
        (">50%", 2),
        ("evident discomfort", 1),
        ("less than 50%", 1),
    )
    return next((severity for marker, severity in rules if marker in normalized), 0)


def _thermal_severity(temperature: float, relative_humidity: float) -> int:
    try:
        _, di_condition = calc_discomfort_index(temperature, relative_humidity)
        _, humidex_condition = calc_hum_index(temperature, relative_humidity)
    except Exception as exc:  # pythermalcomfort rejects physically invalid points
        logger.debug("Thermal comfort calculation skipped: %s", exc)
        return 0
    return max(
        _thermal_condition_severity(di_condition),
        _thermal_condition_severity(humidex_condition),
    )


def _split_contiguous_points(
    points: list[tuple[datetime | date, float]],
    *,
    max_gap: timedelta,
) -> list[list[tuple[datetime | date, float]]]:
    return contiguous_groups(
        points,
        time_key=lambda row: row[0],
        max_gap=max_gap,
    )


def _multiscale_window_sizes(length: int) -> list[int]:
    """Return deterministic geometric scales, including both endpoints."""
    if length <= 0:
        return []
    sizes = {1, length}
    size = 1
    while size < length:
        size = min(length, max(size + 1, math.ceil(size * 1.6)))
        sizes.add(size)
    return sorted(sizes)


def _best_multiscale_interval(
    points: list[tuple[datetime | date, float]],
    *,
    signal,
    max_gap: timedelta,
) -> _IntervalResult | None:
    """Search multiple interval scales without fixing a display horizon.

    The scan rewards accumulated burden, density, and repeat support. Signals
    are capped for interval selection so one extreme point cannot automatically
    displace a sustained pattern; uncapped values remain available for severity.
    """
    best: tuple[float, int, int, datetime | date, _IntervalResult] | None = None
    for sequence in _split_contiguous_points(points, max_gap=max_gap):
        values = [max(0.0, float(signal(value))) for _, value in sequence]
        capped = [min(value, 1.0) for value in values]
        prefix_sum = [0.0]
        prefix_count = [0]
        for value, capped_value in zip(values, capped):
            prefix_sum.append(prefix_sum[-1] + capped_value)
            prefix_count.append(prefix_count[-1] + int(value > 0))

        for width in _multiscale_window_sizes(len(sequence)):
            for start in range(0, len(sequence) - width + 1):
                end = start + width
                signal_sum = prefix_sum[end] - prefix_sum[start]
                signal_count = prefix_count[end] - prefix_count[start]
                if signal_count == 0:
                    continue
                density = signal_count / width
                scan_score = (
                    signal_sum / math.sqrt(width)
                    + 0.25 * density * math.log2(width + 1)
                )
                interval = _IntervalResult(
                    points=tuple(sequence[start:end]),
                    score=scan_score,
                    signal_count=signal_count,
                    signal_sum=signal_sum,
                )
                key = (
                    scan_score,
                    signal_count,
                    width,
                    sequence[end - 1][0],
                    interval,
                )
                if best is None or key[:4] > best[:4]:
                    best = key
    return best[-1] if best is not None else None


def _episode_distance(left: _Episode, right: _Episode) -> float:
    hour_weight = 0.15 if left.horizon == "short_term" else 0.0
    return (
        0.30 * abs(left.magnitude - right.magnitude)
        + 0.20 * abs(left.burden - right.burden)
        + 0.20 * abs(left.persistence - right.persistence)
        + hour_weight * _cyclic_distance(left.hour_phase, right.hour_phase)
        + (0.30 - hour_weight)
        * _cyclic_distance(left.weekday_phase, right.weekday_phase)
    )


def _agglomerative_episode_clusters(
    episodes: list[_Episode],
    *,
    distance_threshold: float = 0.28,
) -> list[list[_Episode]]:
    return average_linkage_clusters(
        episodes,
        distance=_episode_distance,
        sort_key=lambda item: (item.start, item.room_id, item.id),
        distance_threshold=distance_threshold,
        max_items=120,
    )


def _recent_iaq_candidate(
    source: _DeviceSources,
    metric: Literal["co2", "pm25"],
) -> _Candidate | None:
    history = source.recent_iaq
    if history is None:
        return None
    policy = _IAQ_POLICY[metric]
    threshold = get_iaq_threshold(metric, "short")
    all_points = [
        (item.event_time, value)
        for item in history.items
        if is_local_school_hour(item.event_time)
        and (value := _reading_value(item, metric)) is not None
    ]
    interval = _best_multiscale_interval(
        all_points,
        signal=lambda value: value / threshold - 1.0,
        max_gap=timedelta(minutes=90),
    )
    if interval is None:
        return None

    latest_sample = max((stamp for stamp, _ in all_points), default=None)
    episode = list(interval.points)
    ratios = [value / threshold for _, value in episode]
    components = {
        "magnitude": _clamp01(_percentile(ratios, 0.9) - 1.0),
        "frequency": _clamp01(interval.signal_count / max(1, len(all_points))),
        "persistence": _clamp01(interval.signal_count / 4.0),
        "recency": math.exp(
            -max(0.0, (latest_sample - episode[-1][0]).total_seconds()) / (24 * 3600)
        ) if latest_sample is not None else 0.0,
        "spread": 0.0,
        "recurrence": 0.0,
        "representativeness": _clamp01(interval.signal_count / len(episode)),
        "confidence": _clamp01(len(all_points) / 12.0),
    }
    peak = max(value for _, value in episode)
    ratio = peak / threshold
    duration = interval.signal_count
    active = latest_sample is not None and episode[-1][0] == latest_sample
    index_score = _criticality_from_components(components)
    hard_critical = ratio >= 2.5 and duration >= 2
    label = policy["label"]
    room = source.device.label
    state = "continues in the latest occupied sample" if active else "was detected"
    return _Candidate(
        id=f"short-{metric}-{source.device.room_id}",
        horizon="short_term",
        kind=metric,
        severity=_severity_from_criticality(index_score, hard_critical=hard_critical),
        title=f"{label} episode in {room}",
        summary=(
            f"A multi-scale scan found an occupied-hour {label} pattern that {state} "
            f"across {duration} elevated hour{'s' if duration != 1 else ''}."
        ),
        period_label=_format_period(episode[0][0], episode[-1][0], resolution="hourly"),
        room_ids={source.device.room_id},
        room_labels={room},
        evidence=[
            ("Peak", f"{peak:.0f} {policy['unit']}"),
            ("Configured limit", f"{threshold:.0f} {policy['unit']}"),
            ("Detected by", "Multi-scale interval scan"),
        ],
        recommendation=_iaq_recommendation(metric, active=active, multi_room=False),
        index_score=max(index_score, 75 if hard_critical else 0),
        components=components,
        start=episode[0][0],
        end=episode[-1][0],
        active=active,
        burden=float(duration),
        peak_ratio=ratio,
    )


def _long_term_iaq_candidate(
    source: _DeviceSources,
    metric: Literal["co2", "pm25"],
) -> _Candidate | None:
    history = source.daily_iaq
    if history is None:
        return None
    policy = _IAQ_POLICY[metric]
    threshold = get_iaq_threshold(metric, "long")
    values_by_date: dict[date, float] = {}
    for item in history.items:
        value = _reading_value(item, metric)
        if value is not None:
            values_by_date[item.event_time.date()] = value
    if not values_by_date:
        return None

    all_points = sorted(values_by_date.items())
    interval = _best_multiscale_interval(
        all_points,
        signal=lambda value: value / threshold - 1.0,
        max_gap=timedelta(days=3),
    )
    if interval is None:
        return None
    exceeded = [(day, value) for day, value in all_points if value > threshold]
    affected_days = len(exceeded)
    coverage_days = len(values_by_date)
    burden = affected_days / coverage_days
    latest_day = max(values_by_date)
    cluster = list(interval.points)
    ratios = [value / threshold for _, value in cluster]
    components = {
        "magnitude": _clamp01(_percentile(ratios, 0.9) - 1.0),
        "frequency": _clamp01(burden),
        "persistence": _clamp01(interval.signal_count / 5.0),
        "recency": math.exp(-max(0, (latest_day - cluster[-1][0]).days) / 30.0),
        "spread": 0.0,
        "recurrence": _clamp01(affected_days / 8.0),
        "representativeness": _clamp01(interval.signal_count / len(cluster)),
        "confidence": _clamp01(coverage_days / 30.0),
    }
    peak = max(value for _, value in cluster)
    ratio = peak / threshold
    # A single marginal day is retained as balanced positive evidence, not a risk card.
    if interval.signal_count < 2 and ratio < 1.5:
        return None

    index_score = _criticality_from_components(components)
    hard_critical = ratio >= 3.0 and interval.signal_count >= 2
    severity = _severity_from_criticality(index_score, hard_critical=hard_critical)
    label = policy["label"]
    room = source.device.label
    return _Candidate(
        id=f"long-{metric}-{source.device.room_id}",
        horizon="long_term",
        kind=metric,
        severity=severity,
        title=f"Recurring {label} pattern in {room}",
        summary=(
            f"Daily {label} averages exceeded the configured limit on "
            f"{affected_days} of {coverage_days} monitored days."
        ),
        period_label=_format_period(cluster[0][0], cluster[-1][0], resolution="daily"),
        room_ids={source.device.room_id},
        room_labels={room},
        evidence=[
            ("Affected days", f"{affected_days} of {coverage_days} days"),
            ("Highest daily avg", f"{peak:.0f} {policy['unit']}"),
            ("Detected by", "Multi-scale interval scan"),
        ],
        recommendation=_iaq_recommendation(metric, active=False, multi_room=False),
        index_score=max(index_score, 75 if hard_critical else 0),
        components=components,
        start=cluster[0][0],
        end=cluster[-1][0],
        burden=burden * 100,
        peak_ratio=ratio,
    )


def _thermal_points(history: DeviceHistoryResponse | None) -> list[tuple[datetime, int]]:
    if history is None:
        return []
    points: list[tuple[datetime, int]] = []
    for item in history.items:
        if not is_local_school_hour(item.event_time):
            continue
        temperature = _reading_value(item, "temperature")
        humidity = _reading_value(item, "relative_humidity")
        if temperature is None or humidity is None:
            continue
        points.append((item.event_time, _thermal_severity(temperature, humidity)))
    return sorted(points, key=lambda row: row[0])


def _recent_thermal_candidate(source: _DeviceSources) -> _Candidate | None:
    points = _thermal_points(source.thermal_hourly)
    if not points:
        return None
    latest = points[-1][0]
    recent_cutoff = latest - timedelta(hours=RECENT_HOURLY_LIMIT)
    recent = [(stamp, severity) for stamp, severity in points if stamp >= recent_cutoff]
    interval = _best_multiscale_interval(
        [(stamp, float(severity)) for stamp, severity in recent],
        signal=lambda severity: severity / 4.0,
        max_gap=timedelta(minutes=90),
    )
    if interval is None:
        return None
    episode = list(interval.points)
    maximum = int(max(value for _, value in episode))
    duration = interval.signal_count
    active = episode[-1][0] == latest
    components = {
        "magnitude": _clamp01(maximum / 4.0),
        "frequency": _clamp01(duration / max(1, len(recent))),
        "persistence": _clamp01(duration / 4.0),
        "recency": math.exp(
            -max(0.0, (latest - episode[-1][0]).total_seconds()) / (24 * 3600)
        ),
        "spread": 0.0,
        "recurrence": 0.0,
        "representativeness": _clamp01(duration / len(episode)),
        "confidence": _clamp01(len(recent) / 12.0),
    }
    index_score = _criticality_from_components(components)
    hard_critical = maximum >= 4 and duration >= 2
    severity = _severity_from_criticality(index_score, hard_critical=hard_critical)
    room = source.device.label
    return _Candidate(
        id=f"short-thermal-{source.device.room_id}",
        horizon="short_term",
        kind="thermal",
        severity=severity,
        title=f"Thermal discomfort period in {room}",
        summary=(
            f"A multi-scale scan detected thermal discomfort across {duration} occupied "
            f"hour{'s' if duration != 1 else ''}"
            f"{' and remains present in the latest sample' if active else ''}."
        ),
        period_label=_format_period(episode[0][0], episode[-1][0], resolution="hourly"),
        room_ids={source.device.room_id},
        room_labels={room},
        evidence=[
            ("Discomfort period", f"{duration} occupied hour{'s' if duration != 1 else ''}"),
            ("Highest severity", _thermal_severity_label(maximum)),
            ("Detected by", "Multi-scale · DI + Humidex"),
        ],
        recommendation=_thermal_recommendation(active=active, multi_room=False),
        index_score=max(index_score, 75 if hard_critical else 0),
        components=components,
        start=episode[0][0],
        end=episode[-1][0],
        active=active,
        burden=float(duration),
        peak_ratio=float(maximum),
    )


def _daily_thermal_summaries(
    points: list[tuple[datetime, int]],
) -> list[tuple[date, int, int, int, int]]:
    """Return day, occupied hours, discomfort hours, severe hours, longest episode."""
    by_day: dict[date, list[tuple[datetime, int]]] = {}
    for stamp, severity in points:
        by_day.setdefault(stamp.date(), []).append((stamp, severity))
    summaries: list[tuple[date, int, int, int, int]] = []
    for day, day_points in sorted(by_day.items()):
        day_points.sort(key=lambda row: row[0])
        discomfort_hours = sum(severity > 0 for _, severity in day_points)
        severe_hours = sum(severity >= 3 for _, severity in day_points)
        longest = 0
        current = 0
        previous: datetime | None = None
        for stamp, severity in day_points:
            if severity <= 0:
                current = 0
                previous = stamp
                continue
            current = current + 1 if previous and stamp - previous <= timedelta(minutes=90) else 1
            longest = max(longest, current)
            previous = stamp
        summaries.append((day, len(day_points), discomfort_hours, severe_hours, longest))
    return summaries


def _long_term_thermal_candidate(source: _DeviceSources) -> _Candidate | None:
    thermal_points = _thermal_points(source.thermal_hourly)
    daily = _daily_thermal_summaries(thermal_points)
    if not daily:
        return None
    affected = [row for row in daily if row[2] > 0]
    if not affected:
        return None
    total_hours = sum(row[1] for row in daily)
    discomfort_hours = sum(row[2] for row in daily)
    severe_hours = sum(row[3] for row in daily)
    rate = discomfort_hours / total_hours if total_hours else 0.0
    if len(affected) < 2 and rate < 0.25:
        return None

    daily_signal = [
        (row[0], row[2] / max(1, row[1]))
        for row in daily
    ]
    interval = _best_multiscale_interval(
        daily_signal,
        signal=lambda discomfort_share: discomfort_share,
        max_gap=timedelta(days=3),
    )
    if interval is None:
        return None
    cluster = list(interval.points)
    longest = max(row[4] for row in affected)
    latest_day = max(row[0] for row in daily)
    max_level = max((severity for _, severity in thermal_points), default=0)
    components = {
        "magnitude": _clamp01(max_level / 4.0),
        "frequency": _clamp01(rate),
        "persistence": _clamp01(longest / 5.0),
        "recency": math.exp(-max(0, (latest_day - cluster[-1][0]).days) / 30.0),
        "spread": 0.0,
        "recurrence": _clamp01(len(affected) / 8.0),
        "representativeness": _clamp01(interval.signal_count / len(cluster)),
        "confidence": _clamp01(len(daily) / 10.0),
    }
    index_score = _criticality_from_components(components)
    hard_critical = max_level >= 4 and severe_hours >= 3
    severity = _severity_from_criticality(index_score, hard_critical=hard_critical)
    room = source.device.label
    return _Candidate(
        id=f"long-thermal-{source.device.room_id}",
        horizon="long_term",
        kind="thermal",
        severity=severity,
        title=f"Recurring thermal discomfort in {room}",
        summary=(
            f"Daily comfort summaries show discomfort on {len(affected)} of "
            f"{len(daily)} monitored school days."
        ),
        period_label=_format_period(cluster[0][0], cluster[-1][0], resolution="daily derived"),
        room_ids={source.device.room_id},
        room_labels={room},
        evidence=[
            ("Discomfort hours", f"{discomfort_hours} of {total_hours} hours"),
            ("Affected days", f"{len(affected)} of {len(daily)} days"),
            ("Detected by", "Multi-scale daily scan"),
        ],
        recommendation=_thermal_recommendation(active=False, multi_room=False),
        index_score=max(index_score, 75 if hard_critical else 0),
        components=components,
        start=cluster[0][0],
        end=cluster[-1][0],
        burden=rate * 100,
        peak_ratio=1.0 + severe_hours / max(1, total_hours),
    )


def _thermal_severity_label(level: int) -> str:
    return {1: "Mild", 2: "Elevated", 3: "Severe", 4: "Extreme"}.get(level, "Comfortable")


def _iaq_recommendation(metric: str, *, active: bool, multi_room: bool) -> str:
    if metric == "co2":
        if multi_room:
            return "Prioritise a school-wide ventilation check, then verify airflow and occupancy schedules in the affected rooms."
        if active:
            return "Increase safe ventilation now, then check whether occupancy or restricted airflow explains the episode."
        return "Review ventilation and occupancy schedules for the affected period, then verify the next occupied-day readings."
    if multi_room:
        return "Check shared outdoor-air, cleaning, and combustion-related sources before investigating room-specific sources."
    if active:
        return "Reduce likely particle sources, ventilate safely, and verify that the next occupied-hour reading falls below the limit."
    return "Inspect cleaning, outdoor-air, and local particle sources associated with the affected days."


def _thermal_recommendation(*, active: bool, multi_room: bool) -> str:
    if multi_room:
        return "Review the shared heating, cooling, ventilation, and solar-gain schedule, then prioritise the most affected rooms."
    if active:
        return "Adjust temperature or airflow now and check the room again during the next occupied hour."
    return "Review HVAC timing, airflow, shading, and occupancy during the affected periods."


def _best_per_room(candidates: list[_Candidate]) -> list[_Candidate]:
    grouped: dict[tuple[str, str, str], _Candidate] = {}
    for candidate in candidates:
        if len(candidate.room_ids) != 1:
            continue
        room_id = sorted(candidate.room_ids)[0]
        key = (candidate.horizon, candidate.kind, room_id)
        current = grouped.get(key)
        if current is None or candidate.priority > current.priority:
            grouped[key] = candidate
    return list(grouped.values())


def _fuse_across_rooms(
    candidates: list[_Candidate],
    *,
    total_rooms: int,
) -> list[_Candidate]:
    grouped: dict[tuple[str, str], list[_Candidate]] = {}
    pre_fused = [candidate for candidate in candidates if len(candidate.room_ids) > 1]
    for candidate in _best_per_room(candidates):
        grouped.setdefault((candidate.horizon, candidate.kind), []).append(candidate)

    fused: list[_Candidate] = list(pre_fused)
    for (horizon, kind), group in grouped.items():
        rooms = set().union(*(candidate.room_ids for candidate in group))
        worst = max(group, key=lambda candidate: candidate.priority)
        components = {
            name: worst.components.get(name, 0.0)
            for name in ("magnitude", "frequency", "persistence", "recency")
        }
        components["spread"] = len(rooms) / max(1, total_rooms)
        components["recurrence"] = max(
            candidate.components.get("recurrence", 0.0) for candidate in group
        )
        components["representativeness"] = max(
            candidate.components.get("representativeness", 0.0) for candidate in group
        )
        components["confidence"] = max(
            candidate.components.get("confidence", 1.0) for candidate in group
        )
        index_score = _criticality_from_components(components)
        hard_critical = any(candidate.severity == "critical" for candidate in group)
        index_score = max(index_score, 75 if hard_critical else 0)
        severity = _severity_from_criticality(index_score, hard_critical=hard_critical)
        if len(rooms) < 2:
            candidate = group[0]
            candidate.components = components
            candidate.index_score = index_score
            candidate.severity = severity
            fused.extend(group)
            continue
        labels = set().union(*(candidate.room_labels for candidate in group))
        detection_method = next(
            (
                value
                for label, value in worst.evidence
                if label in {"Detected by", "Method"}
            ),
            "Deterministic index",
        )
        starts = [candidate.start for candidate in group if candidate.start is not None]
        ends = [candidate.end for candidate in group if candidate.end is not None]
        start = min(starts) if starts else None
        end = max(ends) if ends else None
        metric_label = _IAQ_POLICY.get(kind, {}).get("label", "thermal discomfort")
        period_resolution = "hourly" if horizon == "short_term" else (
            "daily derived" if kind == "thermal" else "daily"
        )
        if horizon == "short_term":
            title = f"{metric_label[0].upper() + metric_label[1:]} issue across {len(rooms)} rooms"
            summary = (
                f"Related occupied-hour episodes were detected across {len(rooms)} "
                "rooms, so the building-wide signal is prioritised over duplicate room cards."
            )
        else:
            title = f"Recurring {metric_label} pattern across {len(rooms)} rooms"
            summary = (
                f"Daily summaries identify a repeated pattern in {len(rooms)} rooms "
                "within the observed evidence period."
            )
        fused.append(
            _Candidate(
                id=f"{horizon}-{kind}-school",
                horizon=horizon,
                kind=kind,
                severity=severity,
                title=title,
                summary=summary,
                period_label=_format_period(start, end, resolution=period_resolution),
                room_ids=rooms,
                room_labels=labels,
                evidence=[
                    ("Affected rooms", f"{len(rooms)}"),
                    ("Highest priority", severity.capitalize()),
                    ("Detected by", detection_method),
                ],
                recommendation=(
                    _thermal_recommendation(active=any(c.active for c in group), multi_room=True)
                    if kind == "thermal"
                    else _iaq_recommendation(
                        kind,
                        active=any(c.active for c in group),
                        multi_room=True,
                    )
                ),
                index_score=index_score,
                components=components,
                start=start,
                end=end,
                active=any(candidate.active for candidate in group),
                burden=sum(candidate.burden for candidate in group),
                peak_ratio=max(candidate.peak_ratio for candidate in group),
            )
        )
    return fused


def _iaq_series_by_room(
    sources: list[_DeviceSources],
    *,
    horizon: Literal["short_term", "long_term"],
    metric: Literal["co2", "pm25"],
) -> dict[str, tuple[str, list[tuple[datetime | date, float]]]]:
    grouped: dict[tuple[str, datetime | date], list[float]] = {}
    labels_by_room: dict[str, str] = {}
    for source in sources:
        history = source.recent_iaq if horizon == "short_term" else source.daily_iaq
        if history is None:
            continue
        for item in history.items:
            if horizon == "short_term" and not is_local_school_hour(item.event_time):
                continue
            value = _reading_value(item, metric)
            if value is None:
                continue
            bucket = item.event_time if horizon == "short_term" else item.event_time.date()
            grouped.setdefault((source.device.room_id, bucket), []).append(value)
            labels_by_room[source.device.room_id] = source.device.label
    series: dict[str, tuple[str, list[tuple[datetime | date, float]]]] = {}
    for room_id, label in labels_by_room.items():
        points = sorted(
            (
                (bucket, sum(values) / len(values))
                for (candidate_room, bucket), values in grouped.items()
                if candidate_room == room_id
            ),
            key=lambda row: row[0],
        )
        series[room_id] = (label, points)
    return series


def _deduped_iaq_points(
    sources: list[_DeviceSources],
    *,
    horizon: Literal["short_term", "long_term"],
    metric: Literal["co2", "pm25"],
) -> tuple[list[tuple[datetime | date, float]], set[str], set[str]]:
    series = _iaq_series_by_room(sources, horizon=horizon, metric=metric)
    points = [point for _, room_points in series.values() for point in room_points]
    labels = {label for label, _ in series.values()}
    rooms = set(series)
    return points, rooms, labels


def _latest_regime_shift(
    points: list[tuple[datetime | date, float]],
    *,
    threshold: float,
    min_segment: int,
) -> dict[str, object] | None:
    ordered = sorted(points, key=lambda row: row[0])
    normalized = [value / threshold for _, value in ordered]
    change_points = _optimal_change_points(normalized, min_segment=min_segment)
    if not change_points:
        return None
    change_point = change_points[-1]
    previous_start = change_points[-2] if len(change_points) > 1 else 0
    before = ordered[previous_start:change_point]
    after = ordered[change_point:]
    before_mean = sum(value for _, value in before) / len(before)
    after_mean = sum(value for _, value in after) / len(after)
    delta_ratio = (after_mean - before_mean) / threshold
    if abs(delta_ratio) < 0.15:
        return None
    return {
        "before": before,
        "after": after,
        "before_mean": before_mean,
        "after_mean": after_mean,
        "delta_ratio": delta_ratio,
        "change_points": len(change_points),
    }


def _iaq_change_point_candidates(
    sources: list[_DeviceSources],
    *,
    horizon: Literal["short_term", "long_term"],
    metric: Literal["co2", "pm25"],
    total_rooms: int,
) -> list[_Candidate]:
    policy = _IAQ_POLICY[metric]
    threshold = get_iaq_threshold(
        metric,
        "short" if horizon == "short_term" else "long",
    )
    min_segment = 3 if horizon == "short_term" else 7
    candidates: list[_Candidate] = []
    for room_id, (room_label, points) in _iaq_series_by_room(
        sources,
        horizon=horizon,
        metric=metric,
    ).items():
        shift = _latest_regime_shift(
            points,
            threshold=threshold,
            min_segment=min_segment,
        )
        if shift is None or float(shift["delta_ratio"]) <= 0:
            continue
        after = list(shift["after"])
        after_values = [value for _, value in after]
        if _percentile(after_values, 0.9) <= threshold:
            continue
        delta_ratio = float(shift["delta_ratio"])
        exceedance_count = sum(value > threshold for value in after_values)
        components = {
            "magnitude": _clamp01(_percentile(after_values, 0.9) / threshold - 1.0),
            "frequency": _clamp01(exceedance_count / len(after_values)),
            "persistence": _clamp01(len(after_values) / (4.0 if horizon == "short_term" else 14.0)),
            "recency": 1.0,
            "spread": 1.0 / max(1, total_rooms),
            "recurrence": 0.0,
            "representativeness": _clamp01(len(after_values) / len(points)),
            "confidence": _clamp01(abs(delta_ratio) / 0.5),
        }
        index_score = _criticality_index(
            **{name: components[name] for name in ("magnitude", "frequency", "persistence", "recency", "spread")}
        )
        label = policy["label"]
        candidates.append(
            _Candidate(
                id=f"{horizon}-{metric}-{room_id}-change",
                horizon=horizon,
                kind=metric,
                severity=_severity_from_criticality(index_score),
                title=f"Upward {label} regime shift in {room_label}",
                summary=(
                    f"Deterministic change-point detection found a sustained {delta_ratio * 100:.0f}% "
                    "upward shift relative to the configured limit."
                ),
                period_label=_format_period(
                    after[0][0],
                    after[-1][0],
                    resolution="hourly regime" if horizon == "short_term" else "daily regime",
                ),
                room_ids={room_id},
                room_labels={room_label},
                evidence=[
                    ("Previous mean", f"{float(shift['before_mean']):.0f} {policy['unit']}"),
                    ("Current mean", f"{float(shift['after_mean']):.0f} {policy['unit']}"),
                    ("Detected by", "Penalised change point"),
                ],
                recommendation=_iaq_recommendation(metric, active=True, multi_room=False),
                index_score=index_score,
                components=components,
                start=after[0][0],
                end=after[-1][0],
                active=True,
                burden=exceedance_count,
                peak_ratio=max(after_values) / threshold,
            )
        )
    return candidates


def _iaq_episodes(
    sources: list[_DeviceSources],
    *,
    horizon: Literal["short_term", "long_term"],
    metric: Literal["co2", "pm25"],
) -> tuple[list[_Episode], int, datetime | date | None]:
    threshold = get_iaq_threshold(
        metric,
        "short" if horizon == "short_term" else "long",
    )
    episodes: list[_Episode] = []
    total_points = 0
    latest: datetime | date | None = None
    max_gap = timedelta(minutes=90) if horizon == "short_term" else timedelta(days=3)
    persistence_scale = 4.0 if horizon == "short_term" else 5.0
    for room_id, (room_label, points) in _iaq_series_by_room(
        sources,
        horizon=horizon,
        metric=metric,
    ).items():
        total_points += len(points)
        if points:
            latest = max(latest, points[-1][0]) if latest is not None else points[-1][0]
        exceeded = [(stamp, value) for stamp, value in points if value > threshold]
        for index, group in enumerate(_split_contiguous_points(exceeded, max_gap=max_gap)):
            ratios = [value / threshold for _, value in group]
            start = group[0][0]
            hour_phase = start.hour / 24.0 if isinstance(start, datetime) else 0.0
            weekday_phase = start.weekday() / 7.0
            episodes.append(
                _Episode(
                    id=f"{horizon}-{metric}-{room_id}-{index}",
                    horizon=horizon,
                    kind=metric,
                    room_id=room_id,
                    room_label=room_label,
                    start=start,
                    end=group[-1][0],
                    values=tuple(value for _, value in group),
                    magnitude=_clamp01(_percentile(ratios, 0.9) - 1.0),
                    burden=_clamp01(sum(ratio - 1.0 for ratio in ratios) / len(ratios)),
                    persistence=_clamp01(len(group) / persistence_scale),
                    hour_phase=hour_phase,
                    weekday_phase=weekday_phase,
                )
            )
    return episodes, total_points, latest


def _iaq_cluster_candidates(
    sources: list[_DeviceSources],
    *,
    horizon: Literal["short_term", "long_term"],
    metric: Literal["co2", "pm25"],
    total_rooms: int,
) -> list[_Candidate]:
    episodes, total_points, latest = _iaq_episodes(
        sources,
        horizon=horizon,
        metric=metric,
    )
    if len(episodes) < 2 or latest is None:
        return []
    policy = _IAQ_POLICY[metric]
    threshold = get_iaq_threshold(
        metric,
        "short" if horizon == "short_term" else "long",
    )
    candidates: list[_Candidate] = []
    for cluster_index, cluster in enumerate(_agglomerative_episode_clusters(episodes)):
        if len(cluster) < 2:
            continue
        pair_distances = [
            _episode_distance(left, right)
            for left_index, left in enumerate(cluster[:-1])
            for right in cluster[left_index + 1:]
        ]
        average_distance = sum(pair_distances) / len(pair_distances)
        similarity = _clamp01(1.0 - average_distance / 0.28)
        rooms = {episode.room_id for episode in cluster}
        labels = {episode.room_label for episode in cluster}
        values = [value for episode in cluster for value in episode.values]
        end = max(episode.end for episode in cluster)
        start = min(episode.start for episode in cluster)
        if isinstance(latest, datetime) and isinstance(end, datetime):
            recency = math.exp(-max(0.0, (latest - end).total_seconds()) / (24 * 3600))
        else:
            recency = math.exp(-max(0, (latest - end).days) / 30.0)
        components = {
            "magnitude": _clamp01(_percentile([value / threshold for value in values], 0.9) - 1.0),
            "frequency": _clamp01(len(values) / max(1, total_points)),
            "persistence": max(episode.persistence for episode in cluster),
            "recency": recency,
            "spread": len(rooms) / max(1, total_rooms),
            "recurrence": _clamp01(len(cluster) / 4.0),
            "representativeness": _clamp01(len(cluster) / len(episodes)),
            "confidence": similarity,
        }
        index_score = _criticality_index(
            **{name: components[name] for name in ("magnitude", "frequency", "persistence", "recency", "spread")}
        )
        hard_critical = max(values) / threshold >= 2.5 and len(cluster) >= 2
        index_score = max(index_score, 75 if hard_critical else 0)
        severity = _severity_from_criticality(index_score, hard_critical=hard_critical)
        label = policy["label"]
        room_scope = f" across {len(rooms)} rooms" if len(rooms) > 1 else f" in {next(iter(labels))}"
        candidates.append(
            _Candidate(
                id=f"{horizon}-{metric}-cluster-{cluster_index}",
                horizon=horizon,
                kind=metric,
                severity=severity,
                title=f"Recurring {label} episode pattern{room_scope}",
                summary=(
                    f"Hierarchical clustering linked {len(cluster)} similar episodes "
                    f"with {similarity * 100:.0f}% pattern similarity."
                ),
                period_label=_format_period(
                    start,
                    end,
                    resolution="clustered hourly" if horizon == "short_term" else "clustered daily",
                ),
                room_ids=rooms,
                room_labels=labels,
                evidence=[
                    ("Recurring episodes", str(len(cluster))),
                    ("Pattern similarity", f"{similarity * 100:.0f}%"),
                    ("Detected by", "Hierarchical clustering"),
                ],
                recommendation=_iaq_recommendation(
                    metric,
                    active=end == latest,
                    multi_room=len(rooms) > 1,
                ),
                index_score=index_score,
                components=components,
                start=start,
                end=end,
                active=end == latest,
                burden=float(len(values)),
                peak_ratio=max(values) / threshold,
            )
        )
    return candidates


def _best_iaq_improvement(
    sources: list[_DeviceSources],
    *,
    horizon: Literal["short_term", "long_term"],
    metric: Literal["co2", "pm25"],
) -> tuple[str, dict[str, object]] | None:
    threshold = get_iaq_threshold(
        metric,
        "short" if horizon == "short_term" else "long",
    )
    min_segment = 3 if horizon == "short_term" else 7
    improvements: list[tuple[float, datetime | date, str, dict[str, object]]] = []
    for _, (room_label, points) in _iaq_series_by_room(
        sources,
        horizon=horizon,
        metric=metric,
    ).items():
        shift = _latest_regime_shift(
            points,
            threshold=threshold,
            min_segment=min_segment,
        )
        if shift is None or float(shift["delta_ratio"]) >= 0:
            continue
        after = list(shift["after"])
        if _percentile([value for _, value in after], 0.9) > threshold:
            continue
        improvements.append(
            (
                abs(float(shift["delta_ratio"])),
                after[-1][0],
                room_label,
                shift,
            )
        )
    if not improvements:
        return None
    _, _, room_label, shift = max(improvements, key=lambda row: row[:3])
    return room_label, shift


def _positive_iaq_candidate(
    sources: list[_DeviceSources],
    *,
    horizon: Literal["short_term", "long_term"],
    metric: Literal["co2", "pm25"],
) -> _Candidate | None:
    points, rooms, labels = _deduped_iaq_points(
        sources,
        horizon=horizon,
        metric=metric,
    )
    if not points:
        return None
    policy = _IAQ_POLICY[metric]
    threshold = get_iaq_threshold(
        metric,
        "short" if horizon == "short_term" else "long",
    )
    values = [value for _, value in points]
    compliant = sum(value <= threshold for value in values)
    compliance = compliant / len(values)
    p95 = _percentile(values, 0.95)
    peak = max(values)
    raw_margin = max(0.0, 1.0 - p95 / threshold)
    trend = _trend_ratio(points, threshold)
    improvement = _clamp01(0.5 - trend / 0.2)
    target_per_room = 12 if horizon == "short_term" else 30
    coverage = _clamp01(len(points) / max(1, len(rooms) * target_per_room))
    index_score = _stability_index(
        compliance=compliance,
        safety_margin=_clamp01(raw_margin / 0.5),
        improvement=improvement,
        coverage=coverage,
    )
    label = policy["label"]
    resolution = "hourly" if horizon == "short_term" else "daily"
    improvement_shift = _best_iaq_improvement(
        sources,
        horizon=horizon,
        metric=metric,
    )
    if improvement_shift is not None:
        improvement_room, shift = improvement_shift
        title = f"{label} shifted to a lower stable regime"
        summary = (
            f"Change-point detection found a sustained improvement in {improvement_room}; "
            f"the latest regime is {abs(float(shift['delta_ratio'])) * 100:.0f}% lower "
            "relative to the configured limit."
        )
        evidence = [
            ("Previous mean", f"{float(shift['before_mean']):.0f} {policy['unit']}"),
            ("Current mean", f"{float(shift['after_mean']):.0f} {policy['unit']}"),
            ("Detected by", "Penalised change point"),
        ]
    elif compliance == 1:
        title = f"{label} remained within the configured limit"
        summary = (
            f"{compliance * 100:.0f}% of the available {resolution} "
            f"room-buckets were at or below the configured {label} limit."
        )
        evidence = [
            ("Compliance", f"{compliant} of {len(values)} buckets"),
            ("95th percentile", f"{p95:.0f} {policy['unit']}"),
            ("Configured limit", f"{threshold:.0f} {policy['unit']}"),
        ]
    elif compliance >= 0.9:
        title = f"{label} was usually within the configured limit"
        summary = (
            f"{compliance * 100:.0f}% of the available evidence buckets were at or below "
            f"the configured {label} limit."
        )
        evidence = [
            ("Compliance", f"{compliant} of {len(values)} buckets"),
            ("95th percentile", f"{p95:.0f} {policy['unit']}"),
            ("Configured limit", f"{threshold:.0f} {policy['unit']}"),
        ]
    else:
        title = f"No recurring {label} risk pattern was found"
        summary = (
            f"No multi-scale, change-point, or recurring cluster candidate outranked the "
            f"stable evidence; {compliance * 100:.0f}% of buckets met the limit."
        )
        evidence = [
            ("Compliance", f"{compliant} of {len(values)} buckets"),
            ("95th percentile", f"{p95:.0f} {policy['unit']}"),
            ("Search", "Scan · change point · cluster"),
        ]
    return _Candidate(
        id=f"{horizon}-{metric}-stable",
        horizon=horizon,
        kind=metric,
        severity="positive",
        title=title,
        summary=summary,
        period_label=_format_period(
            min(stamp for stamp, _ in points),
            max(stamp for stamp, _ in points),
            resolution=resolution,
        ),
        room_ids=rooms,
        room_labels=labels,
        evidence=evidence,
        recommendation=(
            "Maintain current ventilation practice and keep checking occupied-hour CO2 patterns."
            if metric == "co2"
            else "Maintain current source-control and cleaning practices while continuing PM2.5 monitoring."
        ),
        index_label="Stability index",
        index_score=index_score,
        start=min(stamp for stamp, _ in points),
        end=max(stamp for stamp, _ in points),
        burden=compliance * 100,
        peak_ratio=peak / threshold,
    )


def _deduped_thermal_points(
    sources: list[_DeviceSources],
) -> tuple[list[tuple[str, datetime, int]], set[str], set[str]]:
    grouped: dict[tuple[str, datetime], list[int]] = {}
    labels_by_room: dict[str, str] = {}
    for source in sources:
        for stamp, severity in _thermal_points(source.thermal_hourly):
            grouped.setdefault((source.device.room_id, stamp), []).append(severity)
            labels_by_room[source.device.room_id] = source.device.label
    points = [
        (room_id, stamp, max(values))
        for (room_id, stamp), values in grouped.items()
    ]
    return points, set(labels_by_room), set(labels_by_room.values())


def _thermal_series_by_room(
    sources: list[_DeviceSources],
    *,
    horizon: Literal["short_term", "long_term"],
) -> dict[str, tuple[str, list[tuple[datetime | date, float]]]]:
    all_points, _, _ = _deduped_thermal_points(sources)
    labels_by_room = {
        source.device.room_id: source.device.label
        for source in sources
        if source.thermal_hourly is not None
    }
    by_room: dict[str, list[tuple[datetime, int]]] = {}
    for room_id, stamp, severity in all_points:
        by_room.setdefault(room_id, []).append((stamp, severity))

    series: dict[str, tuple[str, list[tuple[datetime | date, float]]]] = {}
    for room_id, points in by_room.items():
        points.sort(key=lambda row: row[0])
        if horizon == "short_term":
            latest = points[-1][0]
            selected = [
                (stamp, severity / 4.0)
                for stamp, severity in points
                if stamp >= latest - timedelta(hours=RECENT_HOURLY_LIMIT)
            ]
        else:
            daily: dict[date, list[int]] = {}
            for stamp, severity in points:
                daily.setdefault(stamp.date(), []).append(severity)
            selected = [
                (day, sum(level / 4.0 for level in levels) / len(levels))
                for day, levels in sorted(daily.items())
            ]
        series[room_id] = (labels_by_room[room_id], selected)
    return series


def _thermal_change_point_candidates(
    sources: list[_DeviceSources],
    *,
    horizon: Literal["short_term", "long_term"],
    total_rooms: int,
) -> list[_Candidate]:
    min_segment = 3 if horizon == "short_term" else 5
    candidates: list[_Candidate] = []
    for room_id, (room_label, points) in _thermal_series_by_room(
        sources,
        horizon=horizon,
    ).items():
        shift = _latest_regime_shift(
            points,
            threshold=1.0,
            min_segment=min_segment,
        )
        if shift is None or float(shift["delta_ratio"]) <= 0:
            continue
        after = list(shift["after"])
        after_values = [value for _, value in after]
        if _percentile(after_values, 0.9) <= 0:
            continue
        delta_ratio = float(shift["delta_ratio"])
        discomfort_count = sum(value > 0 for value in after_values)
        components = {
            "magnitude": _clamp01(_percentile(after_values, 0.9)),
            "frequency": _clamp01(discomfort_count / len(after_values)),
            "persistence": _clamp01(len(after_values) / (4.0 if horizon == "short_term" else 10.0)),
            "recency": 1.0,
            "spread": 1.0 / max(1, total_rooms),
            "recurrence": 0.0,
            "representativeness": _clamp01(len(after_values) / len(points)),
            "confidence": _clamp01(delta_ratio / 0.5),
        }
        index_score = _criticality_from_components(components)
        candidates.append(
            _Candidate(
                id=f"{horizon}-thermal-{room_id}-change",
                horizon=horizon,
                kind="thermal",
                severity=_severity_from_criticality(index_score),
                title=f"Upward thermal-discomfort regime shift in {room_label}",
                summary=(
                    "Deterministic change-point detection found a sustained increase "
                    f"of {delta_ratio * 100:.0f} percentage points in discomfort burden."
                ),
                period_label=_format_period(
                    after[0][0],
                    after[-1][0],
                    resolution="hourly regime" if horizon == "short_term" else "daily derived regime",
                ),
                room_ids={room_id},
                room_labels={room_label},
                evidence=[
                    ("Previous burden", f"{float(shift['before_mean']) * 100:.0f}%"),
                    ("Current burden", f"{float(shift['after_mean']) * 100:.0f}%"),
                    ("Detected by", "Penalised change point"),
                ],
                recommendation=_thermal_recommendation(active=True, multi_room=False),
                index_score=index_score,
                components=components,
                start=after[0][0],
                end=after[-1][0],
                active=True,
                burden=float(discomfort_count),
                peak_ratio=max(after_values),
            )
        )
    return candidates


def _thermal_episodes(
    sources: list[_DeviceSources],
    *,
    horizon: Literal["short_term", "long_term"],
) -> tuple[list[_Episode], int, datetime | date | None]:
    episodes: list[_Episode] = []
    total_points = 0
    latest: datetime | date | None = None
    max_gap = timedelta(minutes=90) if horizon == "short_term" else timedelta(days=3)
    persistence_scale = 4.0 if horizon == "short_term" else 5.0
    for room_id, (room_label, points) in _thermal_series_by_room(
        sources,
        horizon=horizon,
    ).items():
        total_points += len(points)
        if points:
            latest = max(latest, points[-1][0]) if latest is not None else points[-1][0]
        discomfort = [(stamp, value) for stamp, value in points if value > 0]
        for index, group in enumerate(_split_contiguous_points(discomfort, max_gap=max_gap)):
            start = group[0][0]
            values = [value for _, value in group]
            episodes.append(
                _Episode(
                    id=f"{horizon}-thermal-{room_id}-{index}",
                    horizon=horizon,
                    kind="thermal",
                    room_id=room_id,
                    room_label=room_label,
                    start=start,
                    end=group[-1][0],
                    values=tuple(values),
                    magnitude=_clamp01(_percentile(values, 0.9)),
                    burden=_clamp01(sum(values) / len(values)),
                    persistence=_clamp01(len(group) / persistence_scale),
                    hour_phase=start.hour / 24.0 if isinstance(start, datetime) else 0.0,
                    weekday_phase=start.weekday() / 7.0,
                )
            )
    return episodes, total_points, latest


def _thermal_cluster_candidates(
    sources: list[_DeviceSources],
    *,
    horizon: Literal["short_term", "long_term"],
    total_rooms: int,
) -> list[_Candidate]:
    episodes, total_points, latest = _thermal_episodes(sources, horizon=horizon)
    if len(episodes) < 2 or latest is None:
        return []
    candidates: list[_Candidate] = []
    for cluster_index, cluster in enumerate(_agglomerative_episode_clusters(episodes)):
        if len(cluster) < 2:
            continue
        pair_distances = [
            _episode_distance(left, right)
            for left_index, left in enumerate(cluster[:-1])
            for right in cluster[left_index + 1:]
        ]
        similarity = _clamp01(1.0 - (sum(pair_distances) / len(pair_distances)) / 0.28)
        rooms = {episode.room_id for episode in cluster}
        labels = {episode.room_label for episode in cluster}
        values = [value for episode in cluster for value in episode.values]
        start = min(episode.start for episode in cluster)
        end = max(episode.end for episode in cluster)
        if isinstance(latest, datetime) and isinstance(end, datetime):
            recency = math.exp(-max(0.0, (latest - end).total_seconds()) / (24 * 3600))
        else:
            recency = math.exp(-max(0, (latest - end).days) / 30.0)
        components = {
            "magnitude": _clamp01(_percentile(values, 0.9)),
            "frequency": _clamp01(len(values) / max(1, total_points)),
            "persistence": max(episode.persistence for episode in cluster),
            "recency": recency,
            "spread": len(rooms) / max(1, total_rooms),
            "recurrence": _clamp01(len(cluster) / 4.0),
            "representativeness": _clamp01(len(cluster) / len(episodes)),
            "confidence": similarity,
        }
        index_score = _criticality_from_components(components)
        hard_critical = max(values) >= 1.0 and len(cluster) >= 2
        index_score = max(index_score, 75 if hard_critical else 0)
        severity = _severity_from_criticality(index_score, hard_critical=hard_critical)
        room_scope = f" across {len(rooms)} rooms" if len(rooms) > 1 else f" in {next(iter(labels))}"
        candidates.append(
            _Candidate(
                id=f"{horizon}-thermal-cluster-{cluster_index}",
                horizon=horizon,
                kind="thermal",
                severity=severity,
                title=f"Recurring thermal-discomfort pattern{room_scope}",
                summary=(
                    f"Hierarchical clustering linked {len(cluster)} similar discomfort episodes "
                    f"with {similarity * 100:.0f}% pattern similarity."
                ),
                period_label=_format_period(
                    start,
                    end,
                    resolution="clustered hourly" if horizon == "short_term" else "clustered daily derived",
                ),
                room_ids=rooms,
                room_labels=labels,
                evidence=[
                    ("Recurring episodes", str(len(cluster))),
                    ("Pattern similarity", f"{similarity * 100:.0f}%"),
                    ("Detected by", "Hierarchical clustering"),
                ],
                recommendation=_thermal_recommendation(
                    active=end == latest,
                    multi_room=len(rooms) > 1,
                ),
                index_score=index_score,
                components=components,
                start=start,
                end=end,
                active=end == latest,
                burden=float(len(values)),
                peak_ratio=max(values),
            )
        )
    return candidates


def _positive_thermal_candidate(
    sources: list[_DeviceSources],
    *,
    horizon: Literal["short_term", "long_term"],
) -> _Candidate | None:
    all_points, rooms, labels = _deduped_thermal_points(sources)
    if not all_points:
        return None
    latest = max(stamp for _, stamp, _ in all_points)
    if horizon == "short_term":
        selected = [row for row in all_points if row[1] >= latest - timedelta(hours=RECENT_HOURLY_LIMIT)]
        resolution = "hourly"
        if not selected:
            return None
        comfort_units = sum(severity == 0 for _, _, severity in selected)
        total_units = len(selected)
        comfort_rate = comfort_units / total_units
        timeline: list[tuple[datetime | date, float]] = [
            (stamp, float(severity)) for _, stamp, severity in selected
        ]
        coverage_target = len(rooms) * 12
        period_start: datetime | date = min(stamp for _, stamp, _ in selected)
        period_end: datetime | date = max(stamp for _, stamp, _ in selected)
        evidence = [
            ("Comfortable hours", f"{comfort_units} of {total_units} hours"),
            ("Rooms evaluated", str(len(rooms))),
            ("Method", "DI + Humidex"),
        ]
    else:
        daily_groups: dict[tuple[str, date], list[int]] = {}
        for room_id, stamp, severity in all_points:
            daily_groups.setdefault((room_id, stamp.date()), []).append(severity)
        daily_rates = [
            (day, sum(level > 0 for level in levels) / len(levels))
            for (_, day), levels in daily_groups.items()
        ]
        if not daily_rates:
            return None
        resolution = "daily derived"
        comfort_rate = 1.0 - sum(rate for _, rate in daily_rates) / len(daily_rates)
        comfort_units = round(comfort_rate * len(daily_rates))
        total_units = len(daily_rates)
        timeline = [(day, rate * 4.0) for day, rate in daily_rates]
        coverage_target = len(rooms) * 5
        period_start = min(day for day, _ in daily_rates)
        period_end = max(day for day, _ in daily_rates)
        evidence = [
            ("Comfort share", f"{comfort_rate * 100:.0f}%"),
            ("Daily summaries", str(len(daily_rates))),
            ("Method", "DI + Humidex"),
        ]
    severity_trend = _trend_ratio(timeline, 4.0)
    index_score = _stability_index(
        compliance=comfort_rate,
        safety_margin=comfort_rate,
        improvement=_clamp01(0.5 - severity_trend / 0.2),
        coverage=_clamp01(total_units / max(1, coverage_target)),
    )
    return _Candidate(
        id=f"{horizon}-thermal-stable",
        horizon=horizon,
        kind="thermal",
        severity="positive",
        title="Thermal comfort remained stable",
        summary=(
            f"Comfortable conditions covered {comfort_rate * 100:.0f}% of the available "
            f"occupied-hour evidence without a prioritised discomfort pattern."
        ),
        period_label=_format_period(
            period_start,
            period_end,
            resolution=resolution,
        ),
        room_ids=rooms,
        room_labels=labels,
        evidence=evidence,
        recommendation="Maintain current thermal-management practices and continue occupied-hour monitoring.",
        index_label="Stability index",
        index_score=index_score,
    )


def _missing_evidence_candidate(
    *,
    horizon: Literal["short_term", "long_term"],
    kind: Literal["co2", "pm25", "thermal"],
) -> _Candidate:
    label = _IAQ_POLICY.get(kind, {}).get("label", "Thermal comfort")
    return _Candidate(
        id=f"{horizon}-{kind}-unavailable",
        horizon=horizon,
        kind=kind,
        severity="low",
        title=f"{label} evidence is incomplete",
        summary="There is not enough valid evidence to calculate a reliable deterministic insight.",
        period_label="Insufficient evidence",
        room_ids=set(),
        room_labels=set(),
        evidence=[("Index confidence", "Unavailable")],
        recommendation="Verify sensor availability and history coverage before acting on this metric.",
        index_label="Stability index",
        index_score=0,
    )


def _select_candidates(
    candidates: list[_Candidate],
    *,
    sources: list[_DeviceSources],
    total_rooms: int,
) -> list[_Candidate]:
    fused = _fuse_across_rooms(candidates, total_rooms=total_rooms)
    selected: list[_Candidate] = []
    for horizon in REQUIRED_HORIZONS:
        for kind in REQUIRED_IAQ_KINDS:
            risk_candidates = [
                item for item in fused if item.horizon == horizon and item.kind == kind
            ]
            selected.append(
                max(risk_candidates, key=lambda item: item.priority)
                if risk_candidates
                else _positive_iaq_candidate(sources, horizon=horizon, metric=kind)
                or _missing_evidence_candidate(horizon=horizon, kind=kind)
            )

        thermal_candidates = [
            item for item in fused if item.horizon == horizon and item.kind == "thermal"
        ]
        selected.append(
            max(thermal_candidates, key=lambda item: item.priority)
            if thermal_candidates
            else _positive_thermal_candidate(sources, horizon=horizon)
            or _missing_evidence_candidate(horizon=horizon, kind="thermal")
        )
    return selected


def _candidate_to_schema(candidate: _Candidate) -> EnvironmentalInsightCard:
    return EnvironmentalInsightCard(
        id=candidate.id,
        horizon=candidate.horizon,
        kind=candidate.kind,
        severity=candidate.severity,
        title=candidate.title,
        summary=candidate.summary,
        period_label=candidate.period_label,
        room_labels=sorted(candidate.room_labels),
        affected_rooms=0 if candidate.severity == "positive" else len(candidate.room_ids),
        index_label=candidate.index_label,
        index_score=candidate.index_score,
        evidence=[
            EnvironmentalInsightEvidence(label=label, value=value)
            for label, value in candidate.evidence
        ],
        recommendation=candidate.recommendation,
    )


def _fetch_source(device_id: str, source: str) -> DeviceHistoryResponse:
    if source == "recent_iaq":
        return get_device_history(
            device_id,
            aggregate="avg",
            interval="1h",
            limit=RECENT_HOURLY_LIMIT,
            metrics=METRICS_IAQ_DAILY,
        )
    if source == "daily_iaq":
        return get_device_history(
            device_id,
            aggregate="avg",
            interval="day",
            limit=LONG_TERM_DAILY_LIMIT,
            metrics=METRICS_IAQ_DAILY,
        )
    return get_device_history(
        device_id,
        aggregate="avg",
        interval="1h",
        limit=THERMAL_HOURLY_LIMIT,
        metrics=METRICS_THERMAL_COMFORT,
    )


def _collect_sources(devices: list[SchoolDeviceMetadata]) -> list[_DeviceSources]:
    by_device = {device.id: _DeviceSources(device=device) for device in devices}
    futures = {}
    with ThreadPoolExecutor(max_workers=MAX_UPSTREAM_CONCURRENCY) as executor:
        for device in devices:
            for source in ("recent_iaq", "daily_iaq", "thermal_hourly"):
                future = executor.submit(_fetch_source, device.id, source)
                futures[future] = (device.id, source)
        for future in as_completed(futures):
            device_id, source = futures[future]
            target = by_device[device_id]
            try:
                setattr(target, source, future.result())
            except Exception as exc:
                logger.warning(
                    "School insight source failed for %s/%s: %s",
                    device_id,
                    source,
                    exc,
                )
                target.errors.append(source)
    return list(by_device.values())


def build_school_environmental_insights(
    school_id: str,
    *,
    devices: list[SchoolDeviceMetadata] | None = None,
) -> SchoolEnvironmentalInsightsResponse:
    started = time.perf_counter()
    resolved_devices = (
        devices
        if devices is not None
        else [
            SchoolDeviceMetadata.model_validate(device)
            for device in list_devices_for_school(school_id)
        ]
    )
    sources = _collect_sources(resolved_devices) if resolved_devices else []
    candidates: list[_Candidate] = []
    data_times: list[datetime] = []
    analyzed_rooms: set[str] = set()
    warnings: list[str] = []
    total_rooms = len({device.room_id for device in resolved_devices})

    for source in sources:
        histories = [source.recent_iaq, source.daily_iaq, source.thermal_hourly]
        available = [history for history in histories if history is not None]
        if available:
            analyzed_rooms.add(source.device.room_id)
            data_times.extend(
                item.event_time
                for history in available
                for item in history.items
            )
        if source.errors:
            warnings.append(
                f"{source.device.label}: unavailable {', '.join(sorted(source.errors))} evidence"
            )
        for metric in ("co2", "pm25"):
            recent_candidate = _recent_iaq_candidate(source, metric)
            long_candidate = _long_term_iaq_candidate(source, metric)
            if recent_candidate:
                candidates.append(recent_candidate)
            if long_candidate:
                candidates.append(long_candidate)
        recent_thermal = _recent_thermal_candidate(source)
        long_thermal = _long_term_thermal_candidate(source)
        if recent_thermal:
            candidates.append(recent_thermal)
        if long_thermal:
            candidates.append(long_thermal)

    for horizon in REQUIRED_HORIZONS:
        for metric in REQUIRED_IAQ_KINDS:
            candidates.extend(
                _iaq_change_point_candidates(
                    sources,
                    horizon=horizon,
                    metric=metric,
                    total_rooms=total_rooms,
                )
            )
            candidates.extend(
                _iaq_cluster_candidates(
                    sources,
                    horizon=horizon,
                    metric=metric,
                    total_rooms=total_rooms,
                )
            )
        candidates.extend(
            _thermal_change_point_candidates(
                sources,
                horizon=horizon,
                total_rooms=total_rooms,
            )
        )
        candidates.extend(
            _thermal_cluster_candidates(
                sources,
                horizon=horizon,
                total_rooms=total_rooms,
            )
        )

    selected = _select_candidates(
        candidates,
        sources=sources,
        total_rooms=total_rooms,
    ) if analyzed_rooms else []
    devices_analyzed = sum(
        any(history is not None for history in (source.recent_iaq, source.daily_iaq, source.thermal_hourly))
        for source in sources
    )
    return SchoolEnvironmentalInsightsResponse(
        school_id=school_id,
        generated_at=datetime.now(timezone.utc),
        data_through=max(data_times) if data_times else None,
        total_rooms=total_rooms,
        rooms_analyzed=len(analyzed_rooms),
        total_devices=len(resolved_devices),
        devices_analyzed=devices_analyzed,
        policy_version=POLICY_VERSION,
        insights=[_candidate_to_schema(candidate) for candidate in selected],
        partial=bool(warnings),
        warnings=warnings,
        computation_ms=round((time.perf_counter() - started) * 1000, 1),
    )


def get_school_environmental_insights(
    school_id: str,
) -> SchoolEnvironmentalInsightsResponse:
    return build_school_environmental_insights(school_id)
