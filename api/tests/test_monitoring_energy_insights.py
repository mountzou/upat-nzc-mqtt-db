import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from fastapi.testclient import TestClient

from main import app
from monitoring.routes.auth import get_current_user
from monitoring.schemas import SchoolEnergyInsightsResponse
from monitoring.services.energy import school_insights
from monitoring.services.energy.school_insights import (
    _DailyFeature,
    _EnrichedHour,
    _Episode,
    _HourlyPoint,
    _agglomerative_episode_clusters,
    _carbon_impact_card,
    _dominant_load_candidate,
    _expected_hours_for_local_day,
    _group_contiguous_hours,
    _infer_daily_regimes,
    _learn_hourly_reference,
    _normalize_history,
    _peak_candidate,
    _persistent_baseload_candidate,
    _recent_increase_candidate,
    _resolve_school_series,
    build_school_energy_insights,
    get_school_energy_insights,
)
from monitoring.services.authentication import AuthUserRecord

UTC = timezone.utc
END = datetime(2026, 8, 1, 0, 0, tzinfo=UTC)
START = END - timedelta(days=90)


def _whole_building_device(device_id="whole-1"):
    return {
        "id": device_id,
        "type": "three_phase_meter",
        "room_id": "all_rooms",
        "room_alias": "All rooms",
    }


def _plug(device_id, label):
    return {
        "id": device_id,
        "type": "plug",
        "room_id": label.lower().replace(" ", "-"),
        "room_alias": label,
        "label": label,
    }


def _payload(series_by_device, *, start=START, end=END, omitted=None):
    omitted = omitted or set()
    items = []
    cursor = start
    while cursor < end:
        hour_index = int((cursor - start).total_seconds() // 3600)
        for device_id, value_factory in series_by_device.items():
            if (device_id, cursor) in omitted:
                continue
            value = value_factory(cursor, hour_index)
            if value is None:
                continue
            items.append(
                {
                    "device_id": device_id,
                    "window_start": cursor.isoformat(),
                    "window_end": (cursor + timedelta(hours=1)).isoformat(),
                    "created_at": (cursor + timedelta(hours=1, seconds=5)).isoformat(),
                    "energy_wh": {"total": value},
                }
            )
        cursor += timedelta(hours=1)
    return {"items": list(reversed(items))}


class SchoolEnergyInsightsTests(unittest.TestCase):
    def test_hourly_reference_exposes_hour_of_week_percentiles(self):
        tz = school_insights._school_timezone()
        points = []
        for weeks_ago, value in enumerate((100.0, 200.0, 300.0), start=1):
            stamp = END - timedelta(weeks=weeks_ago)
            points.append(
                _HourlyPoint(
                    stamp=stamp,
                    local_stamp=stamp.astimezone(tz),
                    value_wh=value,
                )
            )

        reference = _learn_hourly_reference(points)
        slot = (points[0].local_stamp.weekday(), points[0].local_stamp.hour)

        self.assertEqual(len(reference.baseline_by_slot), 168)
        self.assertEqual(reference.baseline_by_slot[slot], 140.0)

    @patch("monitoring.services.energy.school_insights.fetch_shelly_hourly_energy")
    def test_live_zero_whole_building_meter_produces_stability_not_missing(self, mock_fetch):
        mock_fetch.return_value = _payload({"whole-1": lambda _stamp, _index: 0.0})

        response = build_school_energy_insights(
            "school-zero",
            devices=[_whole_building_device()],
            now=END,
        )

        self.assertEqual(response.metering_scope, "whole_building")
        self.assertEqual(response.devices_analyzed, 1)
        self.assertEqual(response.coverage_pct, 100.0)
        self.assertFalse(response.partial)
        self.assertEqual(
            {(insight.horizon, insight.kind) for insight in response.insights},
            {
                ("short_term", "carbon_impact"),
                ("short_term", "stability"),
                ("long_term", "stability"),
            },
        )
        carbon = next(item for item in response.insights if item.kind == "carbon_impact")
        self.assertEqual(carbon.severity, "positive")
        self.assertEqual(carbon.evidence[1].value, "0.00 kg CO2e")
        self.assertEqual(response.emissions_factor_kg_per_kwh, 0.285)
        self.assertEqual(response.emissions_factor_version, "project-grid-factor-v1")
        self.assertEqual(len(response.hourly_baseline_profile), 168)
        self.assertTrue(
            all(bucket.baseline_wh == 0.0 for bucket in response.hourly_baseline_profile)
        )

    @patch("monitoring.services.energy.school_insights.fetch_shelly_hourly_energy")
    def test_stale_and_missing_leaf_meters_are_excluded_from_partial_scope(self, mock_fetch):
        stale_cutoff = START + timedelta(hours=100)
        mock_fetch.return_value = _payload(
            {
                "live-zero": lambda _stamp, index: None if index % 10 == 0 else 0.0,
                "stale": lambda stamp, _index: 20.0 if stamp < stale_cutoff else None,
                "missing": lambda _stamp, _index: None,
            }
        )
        devices = [
            _plug("live-zero", "Live zero load"),
            _plug("stale", "Stale load"),
            _plug("missing", "Missing load"),
        ]

        response = build_school_energy_insights(
            "school_10",
            devices=devices,
            now=END,
        )

        self.assertEqual(response.metering_scope, "metered_loads")
        self.assertEqual(response.devices_analyzed, 1)
        self.assertGreater(response.coverage_pct, 85.0)
        self.assertLess(response.coverage_pct, 100.0)
        self.assertTrue(response.partial)
        self.assertTrue(any("monitored loads only" in warning for warning in response.warnings))
        self.assertTrue(any("Stale load excluded" in warning for warning in response.warnings))
        self.assertTrue(any("Missing load excluded" in warning for warning in response.warnings))

    def test_whole_building_meter_prevents_submeter_double_counting(self):
        devices = [_whole_building_device(), _plug("plug-1", "Teachers room")]
        rows = {
            "whole-1": {END - timedelta(hours=1): 100.0},
            "plug-1": {END - timedelta(hours=1): 900.0},
        }
        # Supply enough history for both meters to pass the inclusion gate.
        for offset in range(2, 337):
            stamp = END - timedelta(hours=offset)
            rows["whole-1"][stamp] = 100.0
            rows["plug-1"][stamp] = 900.0

        scope, points, included, warnings, partial = _resolve_school_series(
            devices,
            rows,
            range_start=START,
            range_end=END,
        )

        self.assertEqual(scope, "whole_building")
        self.assertEqual([device["id"] for device in included], ["whole-1"])
        self.assertTrue(all(point.value_wh == 100.0 for point in points))
        self.assertEqual(warnings, [])
        self.assertFalse(partial)

    @patch("monitoring.services.energy.school_insights.fetch_shelly_hourly_energy")
    def test_multiple_whole_building_meters_make_topology_unavailable(self, mock_fetch):
        response = build_school_energy_insights(
            "school-ambiguous",
            devices=[_whole_building_device("whole-1"), _whole_building_device("whole-2")],
            now=END,
        )

        self.assertEqual(response.metering_scope, "unavailable")
        self.assertTrue(response.partial)
        self.assertEqual(response.devices_analyzed, 0)
        self.assertTrue(any("Multiple whole-building" in item for item in response.warnings))
        mock_fetch.assert_not_called()

    @patch("monitoring.services.energy.school_insights.fetch_shelly_hourly_energy")
    def test_after_hours_excess_is_detected_above_learned_baseline(self, mock_fetch):
        def values(stamp, _index):
            local = stamp.astimezone(school_insights._school_timezone())
            if stamp >= END - timedelta(days=14) and local.weekday() < 5 and local.hour in {18, 19, 20}:
                return 5100.0
            return 100.0

        mock_fetch.return_value = _payload({"whole-1": values})

        response = build_school_energy_insights(
            "school-after-hours",
            devices=[_whole_building_device()],
            now=END,
        )

        insight = next(item for item in response.insights if item.kind == "after_hours")
        self.assertEqual(insight.horizon, "short_term")
        self.assertIn("outside regular hours", insight.title)
        self.assertGreater(insight.index_score, 0)
        recent = [item for item in response.insights if item.horizon == "short_term"]
        self.assertEqual(recent[0].kind, "carbon_impact")
        self.assertLessEqual(len(recent), 3)

    def test_carbon_impact_uses_project_factor_and_material_contributors(self):
        tz = school_insights._school_timezone()
        hours = []
        for offset in range(4, 0, -1):
            stamp = END - timedelta(hours=offset)
            point = _HourlyPoint(
                stamp=stamp,
                local_stamp=stamp.astimezone(tz),
                value_wh=1000.0,
                source_values_wh=(("active", 999.0), ("negligible", 1.0)),
            )
            hours.append(
                _EnrichedHour(
                    point=point,
                    baseline_wh=0.0,
                    residual_wh=1000.0,
                    threshold_wh=10.0,
                    significant=True,
                    school_hour=False,
                )
            )

        card = _carbon_impact_card(
            hours,
            scope_labels=["Active Room", "Negligible Room"],
            source_labels={
                "active": "Active Room",
                "negligible": "Negligible Room",
            },
            metering_scope="metered_loads",
            confidence=0.75,
        )

        self.assertIsNotNone(card)
        self.assertEqual(card.kind, "carbon_impact")
        self.assertEqual(card.scope_labels, ["Active Room"])
        self.assertEqual(card.evidence[0].value, "4.0 kWh")
        self.assertEqual(card.evidence[1].value, "1.14 kg CO2e")
        self.assertEqual(card.evidence[2].value, "1.14 kg CO2e")
        self.assertEqual(card.evidence[3].label, "Above-baseline emissions share")
        self.assertEqual(card.evidence[3].value, "100.0%")
        self.assertIn("0.285 kg CO2e/kWh", card.summary)

    @patch("monitoring.services.energy.school_insights.fetch_shelly_hourly_energy")
    def test_after_hours_scope_only_names_material_contributors(self, mock_fetch):
        def active_values(stamp, _index):
            local = stamp.astimezone(school_insights._school_timezone())
            if stamp >= END - timedelta(days=14) and local.weekday() < 5 and local.hour in {18, 19, 20}:
                return 5100.0
            return 100.0

        mock_fetch.return_value = _payload(
            {
                "active": active_values,
                "zero": lambda _stamp, _index: 0.0,
                "negligible": lambda _stamp, _index: 1.0,
            }
        )

        response = build_school_energy_insights(
            "school-attribution",
            devices=[
                _plug("active", "Active Room"),
                _plug("zero", "Zero Room"),
                _plug("negligible", "Negligible Room"),
            ],
            now=END,
        )

        insight = next(item for item in response.insights if item.kind == "after_hours")
        self.assertEqual(insight.scope_labels, ["Active Room"])
        self.assertNotIn("Last 14 complete days", insight.period_label)

    @patch("monitoring.services.energy.school_insights.fetch_shelly_hourly_energy")
    def test_persistent_closed_hour_step_generates_baseload_change_point(self, mock_fetch):
        change_at = END - timedelta(days=20)
        mock_fetch.return_value = _payload(
            {"whole-1": lambda stamp, _index: 200.0 if stamp >= change_at else 100.0}
        )

        response = build_school_energy_insights(
            "school-baseload",
            devices=[_whole_building_device()],
            now=END,
        )

        baseload = next(item for item in response.insights if item.kind == "baseload")
        self.assertEqual(baseload.horizon, "long_term")
        self.assertIn("change point", baseload.summary)

    def test_missing_hour_breaks_episode_instead_of_becoming_zero(self):
        def hour(offset):
            stamp = END - timedelta(hours=offset)
            point = _HourlyPoint(stamp, stamp, 100.0)
            return _EnrichedHour(point, 0.0, 100.0, 1.0, True, False)

        groups = _group_contiguous_hours([hour(5), hour(4), hour(1)])

        self.assertEqual([len(group) for group in groups], [2, 1])

    def test_recurring_similar_episodes_generate_peak_candidate(self):
        hours = []
        for day_offset in (6, 4, 2):
            stamp = END - timedelta(days=day_offset, hours=14)
            point = _HourlyPoint(stamp, stamp, 1100.0)
            hours.append(_EnrichedHour(point, 100.0, 1000.0, 10.0, True, True))
        trailing_stamp = END - timedelta(hours=1)
        trailing = _HourlyPoint(trailing_stamp, trailing_stamp, 100.0)
        hours.append(_EnrichedHour(trailing, 100.0, 0.0, 10.0, False, False))

        candidate = _peak_candidate(
            sorted(hours, key=lambda item: item.point.stamp),
            horizon="short_term",
            scope_labels=["Whole building"],
            metering_scope="whole_building",
            confidence=1.0,
            skip_after_hours_cluster=False,
        )

        self.assertIsNotNone(candidate)
        self.assertEqual(candidate.kind, "recurring_peak")
        self.assertEqual(candidate.evidence[0].value, "3")

    def test_extended_zero_heavy_run_is_inferred_as_low_activity(self):
        features = [
            _DailyFeature(
                day=date,
                total_wh=5000.0 if index < 10 else 0.0,
                baseline_wh=0.0,
                excess_wh=5000.0 if index < 10 else 0.0,
                after_hours_excess_wh=0.0,
                peak_wh=500.0 if index < 10 else 0.0,
                closed_mean_wh=100.0 if index < 10 else 0.0,
                active_hours=6 if index < 10 else 0,
                coverage=1.0,
            )
            for index, date in enumerate(
                (END.date() - timedelta(days=19 - offset) for offset in range(20))
            )
        ]

        _infer_daily_regimes(features)

        self.assertTrue(all(item.regime == "active" for item in features[:10]))
        self.assertTrue(all(item.regime == "low_activity" for item in features[10:]))

    def test_recent_increase_uses_same_weekday_reference_baseline(self):
        local_end_day = END.astimezone(school_insights._school_timezone()).date()
        features = []
        for offset in range(56, 0, -1):
            day = local_end_day - timedelta(days=offset)
            total_wh = 2000.0 if day >= local_end_day - timedelta(days=14) else 1000.0
            features.append(
                _DailyFeature(
                    day=day,
                    total_wh=total_wh,
                    baseline_wh=500.0,
                    excess_wh=total_wh - 500.0,
                    after_hours_excess_wh=0.0,
                    peak_wh=total_wh,
                    closed_mean_wh=10.0,
                    active_hours=1,
                    coverage=1.0,
                )
            )

        candidate = _recent_increase_candidate(
            features,
            range_end=END,
            scope_labels=["Whole building"],
            metering_scope="whole_building",
            confidence=1.0,
        )

        self.assertIsNotNone(candidate)
        self.assertEqual(candidate.kind, "recent_increase")
        self.assertEqual(candidate.evidence[0].value, "+100%")
        self.assertEqual(candidate.evidence[1].value, "28.0 kWh")

    def test_persistent_background_load_is_reported_during_low_activity(self):
        tz = school_insights._school_timezone()
        local_end_day = END.astimezone(tz).date()
        features = []
        hours = []
        for offset in range(10, 0, -1):
            day = local_end_day - timedelta(days=offset)
            features.append(
                _DailyFeature(
                    day=day,
                    total_wh=2400.0,
                    baseline_wh=2400.0,
                    excess_wh=0.0,
                    after_hours_excess_wh=0.0,
                    peak_wh=100.0,
                    closed_mean_wh=100.0,
                    active_hours=0,
                    coverage=1.0,
                    regime="low_activity",
                )
            )
            for hour_value in range(24):
                local_stamp = datetime.combine(
                    day,
                    datetime.min.time(),
                    tzinfo=tz,
                ) + timedelta(hours=hour_value)
                point = _HourlyPoint(
                    stamp=local_stamp.astimezone(UTC),
                    local_stamp=local_stamp,
                    value_wh=100.0,
                )
                hours.append(
                    _EnrichedHour(point, 100.0, 0.0, 1.0, False, False)
                )

        candidate = _persistent_baseload_candidate(
            hours,
            daily=features,
            scope_labels=["Whole building"],
            metering_scope="whole_building",
            confidence=1.0,
        )

        self.assertIsNotNone(candidate)
        self.assertEqual(candidate.kind, "persistent_baseload")
        self.assertEqual(candidate.evidence[0].value, "2.4 kWh/day")
        self.assertEqual(candidate.evidence[1].value, "10 days")

    def test_dominant_load_uses_only_complete_common_meter_hours(self):
        devices = [
            _plug("subdirector", "Subdirector Room"),
            _plug("computer", "Computer Room"),
        ]
        stamps = [END - timedelta(hours=offset) for offset in range(1, 337)]
        rows = {
            "subdirector": {stamp: 100.0 for stamp in stamps},
            "computer": {stamp: 50.0 for stamp in stamps},
        }
        points = [
            _HourlyPoint(
                stamp=stamp,
                local_stamp=stamp.astimezone(school_insights._school_timezone()),
                value_wh=150.0,
            )
            for stamp in sorted(stamps)
        ]

        candidate = _dominant_load_candidate(
            rows,
            included=devices,
            points=points,
            range_end=END,
            confidence=0.75,
        )

        self.assertIsNotNone(candidate)
        self.assertEqual(candidate.kind, "dominant_load")
        self.assertEqual(candidate.scope_labels, ["Subdirector Room"])
        self.assertEqual(candidate.evidence[0].value, "67%")
        self.assertEqual(candidate.evidence[2].value, "100%")
        self.assertNotIn("Last 14 complete days", candidate.period_label)

    @patch("monitoring.services.energy.school_insights.fetch_shelly_hourly_energy")
    def test_continuous_non_zero_baseload_is_stable_without_a_change_point(self, mock_fetch):
        mock_fetch.return_value = _payload({"whole-1": lambda _stamp, _index: 100.0})

        response = build_school_energy_insights(
            "school-continuous",
            devices=[_whole_building_device()],
            now=END,
        )

        self.assertFalse(any(item.kind == "baseload" for item in response.insights))
        self.assertEqual(
            {(item.horizon, item.kind) for item in response.insights},
            {
                ("short_term", "carbon_impact"),
                ("short_term", "stability"),
                ("long_term", "stability"),
            },
        )

    def test_clustering_is_deterministic_when_input_order_changes(self):
        episodes = [
            _Episode(
                id=f"episode-{index}",
                start=END + timedelta(days=index),
                end=END + timedelta(days=index, hours=1),
                excess_wh=1000 + index,
                peak_wh=500,
                duration_hours=2,
                hour_phase=8 / 24,
                weekday_phase=(index % 5) / 7,
                after_hours_share=0.0,
                magnitude=0.5,
                burden=0.4,
                persistence=0.3,
            )
            for index in range(3)
        ]
        episodes.append(
            _Episode(
                id="outlier",
                start=END + timedelta(hours=20),
                end=END + timedelta(hours=20),
                excess_wh=5000,
                peak_wh=2000,
                duration_hours=1,
                hour_phase=20 / 24,
                weekday_phase=6 / 7,
                after_hours_share=1.0,
                magnitude=1.0,
                burden=1.0,
                persistence=0.1,
            )
        )

        first = sorted(sorted(item.id for item in group) for group in _agglomerative_episode_clusters(episodes))
        second = sorted(
            sorted(item.id for item in group)
            for group in _agglomerative_episode_clusters(list(reversed(episodes)))
        )

        self.assertEqual(first, second)
        self.assertIn(["episode-0", "episode-1", "episode-2"], first)

    def test_newest_duplicate_wins_and_invalid_negative_row_is_ignored(self):
        stamp = END - timedelta(hours=1)
        payload = {
            "items": [
                {
                    "device_id": "whole-1",
                    "window_start": stamp.isoformat(),
                    "window_end": END.isoformat(),
                    "created_at": (stamp + timedelta(minutes=5)).isoformat(),
                    "energy_wh": {"total": 100},
                },
                {
                    "device_id": "whole-1",
                    "window_start": stamp.isoformat(),
                    "window_end": END.isoformat(),
                    "created_at": (stamp + timedelta(minutes=10)).isoformat(),
                    "energy_wh": {"total": 250},
                },
                {
                    "device_id": "whole-1",
                    "window_start": (stamp - timedelta(hours=1)).isoformat(),
                    "window_end": stamp.isoformat(),
                    "energy_wh": {"total": -1},
                },
            ]
        }

        rows, _, invalid, duplicates = _normalize_history(
            payload,
            device_ids={"whole-1"},
            start=START,
            end=END,
        )

        self.assertEqual(rows["whole-1"][stamp], 250.0)
        self.assertEqual(invalid, 1)
        self.assertEqual(duplicates, 1)

    def test_history_range_is_half_open(self):
        payload = _payload(
            {"whole-1": lambda _stamp, _index: 10.0},
            start=START - timedelta(hours=1),
            end=END + timedelta(hours=1),
        )

        rows, _, _, _ = _normalize_history(
            payload,
            device_ids={"whole-1"},
            start=START,
            end=END,
        )

        self.assertIn(START, rows["whole-1"])
        self.assertNotIn(END, rows["whole-1"])

    @patch("monitoring.services.energy.school_insights.fetch_shelly_hourly_energy")
    def test_ranked_output_is_deterministic_when_payload_order_changes(self, mock_fetch):
        payload = _payload(
            {
                "whole-1": lambda stamp, _index: (
                    4100.0
                    if stamp >= END - timedelta(days=14)
                    and stamp.astimezone(school_insights._school_timezone()).hour in {18, 19}
                    else 100.0
                )
            }
        )
        mock_fetch.side_effect = [payload, {"items": list(reversed(payload["items"]))}]

        first = build_school_energy_insights(
            "school-order-a", devices=[_whole_building_device()], now=END
        )
        second = build_school_energy_insights(
            "school-order-b", devices=[_whole_building_device()], now=END
        )

        first_ranked = [(item.id, item.index_score, item.severity) for item in first.insights]
        second_ranked = [(item.id, item.index_score, item.severity) for item in second.insights]
        self.assertEqual(first_ranked, second_ranked)

    def test_dst_days_have_23_and_25_expected_hours(self):
        self.assertEqual(_expected_hours_for_local_day(datetime(2026, 3, 29).date()), 23)
        self.assertEqual(_expected_hours_for_local_day(datetime(2026, 10, 25).date()), 25)

    @patch("monitoring.services.energy.school_insights.build_school_energy_insights")
    def test_repeated_requests_are_recomputed(self, mock_build):
        mock_build.return_value = SchoolEnergyInsightsResponse(
            school_id="school-fresh",
            generated_at=END,
            policy_version="energy-insights-v1.4",
            metering_scope="whole_building",
        )

        get_school_energy_insights("school-fresh")
        get_school_energy_insights("school-fresh")

        self.assertEqual(mock_build.call_count, 2)


class SchoolEnergyInsightsRouteTests(unittest.TestCase):
    @patch("monitoring.routes.energy_demand.get_school_energy_insights")
    def test_get_route_exposes_schema_without_response_caching(self, mock_get):
        mock_get.return_value = SchoolEnergyInsightsResponse(
            school_id="school-route",
            generated_at=END,
            data_through=END,
            policy_version="energy-insights-v1.3",
            emissions_factor_kg_per_kwh=0.285,
            emissions_factor_source="SchoolHeroz project grid factor",
            emissions_factor_version="project-grid-factor-v1",
            metering_scope="whole_building",
            coverage_pct=100.0,
            hourly_baseline_profile=[
                {"weekday": 0, "hour": 8, "baseline_wh": 250.0}
            ],
        )
        app.dependency_overrides[get_current_user] = lambda: AuthUserRecord(
            username="test-admin",
            role="system_admin",
        )
        try:
            with TestClient(app) as client:
                response = client.get("/energy/schools/school-route/insights")
        finally:
            app.dependency_overrides.pop(get_current_user, None)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["metering_scope"], "whole_building")
        self.assertEqual(response.json()["emissions_factor_kg_per_kwh"], 0.285)
        self.assertEqual(
            response.json()["hourly_baseline_profile"],
            [{"weekday": 0, "hour": 8, "baseline_wh": 250.0}],
        )
        self.assertEqual(
            response.json()["emissions_factor_version"],
            "project-grid-factor-v1",
        )
        self.assertNotIn("cache-control", response.headers)
        self.assertNotIn("cache_hit", response.json())
        self.assertIn("school-energy-insights;dur=", response.headers["server-timing"])
