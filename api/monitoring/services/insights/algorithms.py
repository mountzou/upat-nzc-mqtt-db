"""Deterministic algorithms shared by the energy and environmental engines.

This module contains only generic numerical mechanics. Domain feature design,
distance weights, thresholds, confidence policy, and user-facing copy remain in
their respective insight engines.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence
from datetime import date, datetime, timedelta
from typing import Any, TypeVar

ItemT = TypeVar("ItemT")


def contiguous_groups(
    items: Sequence[ItemT],
    *,
    time_key: Callable[[ItemT], date | datetime],
    max_gap: timedelta,
) -> list[list[ItemT]]:
    """Split time-ordered items whenever the gap exceeds ``max_gap``."""
    groups: list[list[ItemT]] = []
    for item in sorted(items, key=time_key):
        if not groups or time_key(item) - time_key(groups[-1][-1]) > max_gap:
            groups.append([item])
        else:
            groups[-1].append(item)
    return groups


def percentile(values: Sequence[float], quantile: float) -> float:
    """Linearly interpolate a quantile after clamping it to the 0..1 range."""
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * max(0.0, min(1.0, quantile))
    lower = int(math.floor(position))
    upper = min(len(ordered) - 1, lower + 1)
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _segment_sse(
    prefix_sum: list[float],
    prefix_square_sum: list[float],
    start: int,
    end: int,
) -> float:
    length = end - start
    if length <= 0:
        return 0.0
    total = prefix_sum[end] - prefix_sum[start]
    square_total = prefix_square_sum[end] - prefix_square_sum[start]
    return max(0.0, square_total - total * total / length)


def optimal_change_points(
    values: Sequence[float],
    *,
    min_segment: int = 7,
) -> list[int]:
    """Return exact penalized-SSE piecewise-constant segment starts.

    The dynamic program is deterministic. Returned indices exclude both zero
    and ``len(values)``.
    """
    if min_segment < 1:
        raise ValueError("min_segment must be at least 1")

    length = len(values)
    if length < 2 * min_segment:
        return []

    prefix_sum = [0.0]
    prefix_square_sum = [0.0]
    for value in values:
        numeric = float(value)
        prefix_sum.append(prefix_sum[-1] + numeric)
        prefix_square_sum.append(prefix_square_sum[-1] + numeric * numeric)

    variance = _segment_sse(prefix_sum, prefix_square_sum, 0, length) / length
    if variance <= 1e-9:
        return []

    penalty = max(0.01, 2.0 * math.log(length) * variance)
    costs = [math.inf] * (length + 1)
    previous: list[int | None] = [None] * (length + 1)
    costs[0] = -penalty

    for segment_end in range(min_segment, length + 1):
        starts = [0]
        starts.extend(range(min_segment, segment_end - min_segment + 1))
        for segment_start in starts:
            if (
                costs[segment_start] == math.inf
                or segment_end - segment_start < min_segment
            ):
                continue
            candidate = (
                costs[segment_start]
                + _segment_sse(
                    prefix_sum,
                    prefix_square_sum,
                    segment_start,
                    segment_end,
                )
                + penalty
            )
            if candidate < costs[segment_end] - 1e-12:
                costs[segment_end] = candidate
                previous[segment_end] = segment_start

    if previous[length] is None:
        return []

    result: list[int] = []
    cursor = length
    while previous[cursor] not in (None, 0):
        cursor = int(previous[cursor])
        result.append(cursor)
    return sorted(result)


def cyclic_distance(left: float, right: float) -> float:
    """Normalized shortest distance between two phases on a unit circle."""
    difference = abs(left - right) % 1.0
    return min(difference, 1.0 - difference) * 2.0


def average_linkage_clusters(
    items: Sequence[ItemT],
    *,
    distance: Callable[[ItemT, ItemT], float],
    sort_key: Callable[[ItemT], Any],
    distance_threshold: float,
    max_items: int = 120,
) -> list[list[ItemT]]:
    """Average-linkage hierarchical clustering with stable tie-breaking.

    ``distance`` and ``sort_key`` are supplied by each domain so the shared
    engine owns only the clustering mechanics.
    """
    if max_items < 1:
        raise ValueError("max_items must be at least 1")

    bounded = sorted(items, key=sort_key)[-max_items:]
    clusters: list[tuple[int, ...]] = [
        (index,) for index in range(len(bounded))
    ]

    def average_distance(left: tuple[int, ...], right: tuple[int, ...]) -> float:
        distances = [
            distance(bounded[left_index], bounded[right_index])
            for left_index in left
            for right_index in right
        ]
        return sum(distances) / len(distances)

    while len(clusters) > 1:
        best: tuple[float, tuple[int, ...], tuple[int, ...], int, int] | None = None
        for left_index in range(len(clusters) - 1):
            for right_index in range(left_index + 1, len(clusters)):
                left = clusters[left_index]
                right = clusters[right_index]
                cluster_distance = average_distance(left, right)
                key = (
                    cluster_distance,
                    left,
                    right,
                    left_index,
                    right_index,
                )
                if best is None or key[:3] < best[:3]:
                    best = key

        if best is None or best[0] > distance_threshold:
            break

        _, left, right, left_index, right_index = best
        merged = tuple(sorted(left + right))
        clusters = [
            cluster
            for index, cluster in enumerate(clusters)
            if index not in (left_index, right_index)
        ]
        clusters.append(merged)
        clusters.sort()

    return [
        [bounded[index] for index in cluster]
        for cluster in sorted(clusters)
    ]
