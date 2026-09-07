import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from monitoring.schemas import (
    DeviceHistoryBucketItem,
    DeviceHistoryResponse,
    OverviewReading,
    SchoolDeviceMetadata,
)
from monitoring.services.environment.school_insights import (
    _DeviceSources,
    _Episode,
    _agglomerative_episode_clusters,
    _best_multiscale_interval,
    _criticality_index,
    _daily_thermal_summaries,
    _stability_index,
    build_school_environmental_insights,
    get_school_environmental_insights,
)


def _history(device_id, rows):
    return DeviceHistoryResponse(
        device_id=device_id,
        count=len(rows),
        items=[
            DeviceHistoryBucketItem(
                device_id=device_id,
                event_time=stamp.replace(tzinfo=timezone.utc),
                measurements={
                    metric: OverviewReading(value=value, unit=None)
                    for metric, value in measurements.items()
                },
            )
            for stamp, measurements in rows
        ],
    )


class SchoolEnvironmentalInsightsTests(unittest.TestCase):
    @patch("monitoring.services.environment.school_insights.build_school_environmental_insights")
    def test_repeated_requests_are_recomputed(self, mock_build):
        mock_build.return_value = object()

        get_school_environmental_insights("school-fresh")
        get_school_environmental_insights("school-fresh")

        self.assertEqual(mock_build.call_count, 2)

    def test_multiscale_search_prefers_supported_interval_over_isolated_peak(self):
        points = [
            (datetime(2026, 7, 20, 8), 2.0),
            (datetime(2026, 7, 21, 8), 1.4),
            (datetime(2026, 7, 21, 9), 1.4),
            (datetime(2026, 7, 21, 10), 1.4),
            (datetime(2026, 7, 21, 11), 1.4),
        ]

        result = _best_multiscale_interval(
            points,
            signal=lambda value: value - 1.0,
            max_gap=timedelta(minutes=90),
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.signal_count, 4)
        self.assertEqual(result.points[0][0], datetime(2026, 7, 21, 8))
        self.assertEqual(result.points[-1][0], datetime(2026, 7, 21, 11))

    def test_hierarchical_clustering_groups_similar_episodes_without_randomness(self):
        base = datetime(2026, 7, 20, 8)
        episodes = [
            _Episode(
                id="a",
                horizon="short_term",
                kind="co2",
                room_id="room-a",
                room_label="Room A",
                start=base,
                end=base + timedelta(hours=2),
                values=(900.0, 920.0, 910.0),
                magnitude=0.22,
                burden=0.20,
                persistence=0.75,
                hour_phase=8 / 24,
                weekday_phase=0.0,
            ),
            _Episode(
                id="b",
                horizon="short_term",
                kind="co2",
                room_id="room-b",
                room_label="Room B",
                start=base + timedelta(days=1),
                end=base + timedelta(days=1, hours=2),
                values=(890.0, 930.0, 905.0),
                magnitude=0.21,
                burden=0.19,
                persistence=0.75,
                hour_phase=8 / 24,
                weekday_phase=1 / 7,
            ),
            _Episode(
                id="c",
                horizon="short_term",
                kind="co2",
                room_id="room-c",
                room_label="Room C",
                start=base + timedelta(days=1, hours=11),
                end=base + timedelta(days=1, hours=11),
                values=(1800.0,),
                magnitude=1.0,
                burden=1.0,
                persistence=0.25,
                hour_phase=19 / 24,
                weekday_phase=1 / 7,
            ),
        ]

        first = _agglomerative_episode_clusters(episodes)
        second = _agglomerative_episode_clusters(list(reversed(episodes)))

        first_ids = sorted(sorted(item.id for item in cluster) for cluster in first)
        second_ids = sorted(sorted(item.id for item in cluster) for cluster in second)
        self.assertEqual(first_ids, [["a", "b"], ["c"]])
        self.assertEqual(second_ids, first_ids)

    def test_indices_reward_persistent_risk_and_stable_compliance(self):
        isolated = _criticality_index(
            magnitude=1.0,
            frequency=0.02,
            persistence=0.1,
            recency=0.1,
            spread=0.25,
        )
        persistent = _criticality_index(
            magnitude=0.7,
            frequency=0.6,
            persistence=0.8,
            recency=1.0,
            spread=0.75,
        )
        stable = _stability_index(
            compliance=1.0,
            safety_margin=0.8,
            improvement=0.5,
            coverage=1.0,
        )

        self.assertLess(isolated, persistent)
        self.assertGreaterEqual(persistent, 70)
        self.assertGreaterEqual(stable, 80)

    def test_daily_thermal_summary_preserves_hourly_discomfort_periods(self):
        rows = [
            (datetime(2026, 7, 27, 8), 0),
            (datetime(2026, 7, 27, 9), 2),
            (datetime(2026, 7, 27, 10), 3),
            (datetime(2026, 7, 27, 12), 0),
            (datetime(2026, 7, 28, 8), 1),
        ]

        summaries = _daily_thermal_summaries(rows)

        self.assertEqual(
            summaries,
            [
                (datetime(2026, 7, 27).date(), 4, 2, 1, 2),
                (datetime(2026, 7, 28).date(), 1, 1, 0, 1),
            ],
        )

    @patch("monitoring.services.environment.school_insights._thermal_severity", return_value=0)
    @patch("monitoring.services.environment.school_insights._collect_sources")
    def test_duplicate_sensors_are_deduplicated_and_rooms_are_fused(
        self,
        mock_collect,
        _mock_thermal_severity,
    ):
        devices = [
            SchoolDeviceMetadata(id="sensor-a", label="Teachers Office", room_id="teachers"),
            SchoolDeviceMetadata(id="sensor-b", label="Teachers Office", room_id="teachers"),
            SchoolDeviceMetadata(id="sensor-c", label="Library", room_id="library"),
        ]
        recent_rows = [
            (datetime(2026, 7, 27, 8), {"co2": 820, "pm25": 4}),
            (datetime(2026, 7, 27, 9), {"co2": 950, "pm25": 4}),
            (datetime(2026, 7, 27, 10), {"co2": 900, "pm25": 4}),
        ]
        daily_rows = [
            (datetime(2026, 7, 20), {"co2": 850, "pm25": 4}),
            (datetime(2026, 7, 22), {"co2": 900, "pm25": 4}),
            (datetime(2026, 7, 24), {"co2": 880, "pm25": 4}),
        ]
        thermal_rows = [
            (
                datetime(2026, 7, 27, 8),
                {"temperature": 20, "relative_humidity": 45},
            )
        ]
        mock_collect.return_value = [
            _DeviceSources(
                device=device,
                recent_iaq=_history(device.id, recent_rows),
                daily_iaq=_history(device.id, daily_rows),
                thermal_hourly=_history(device.id, thermal_rows),
            )
            for device in devices
        ]

        response = build_school_environmental_insights("school-test", devices=devices)

        self.assertEqual(response.total_devices, 3)
        self.assertEqual(response.total_rooms, 2)
        self.assertEqual(response.rooms_analyzed, 2)
        self.assertEqual(len(response.insights), 6)
        iaq_slots = {
            (insight.horizon, insight.kind)
            for insight in response.insights
            if insight.kind in {"co2", "pm25"}
        }
        self.assertEqual(
            iaq_slots,
            {
                ("short_term", "co2"),
                ("short_term", "pm25"),
                ("long_term", "co2"),
                ("long_term", "pm25"),
            },
        )
        self.assertTrue(
            any(insight.severity == "positive" for insight in response.insights)
        )
        self.assertTrue(all(0 <= insight.index_score <= 100 for insight in response.insights))
        short_co2 = next(
            insight
            for insight in response.insights
            if insight.horizon == "short_term" and insight.kind == "co2"
        )
        self.assertEqual(short_co2.affected_rooms, 2)
        self.assertEqual(short_co2.room_labels, ["Library", "Teachers Office"])
        self.assertEqual(
            short_co2.title,
            "Recurring CO2 episode pattern across 2 rooms",
        )
        self.assertIn(
            ("Detected by", "Hierarchical clustering"),
            [(item.label, item.value) for item in short_co2.evidence],
        )
        self.assertEqual(short_co2.index_label, "Criticality index")

    @patch("monitoring.services.environment.school_insights.get_device_history")
    def test_collection_uses_hourly_recent_daily_long_term_and_derived_thermal(
        self,
        mock_history,
    ):
        device = SchoolDeviceMetadata(
            id="sensor-a",
            label="Classroom",
            room_id="classroom",
        )
        mock_history.return_value = _history(
            device.id,
            [(datetime(2026, 7, 27, 8), {"co2": 500, "pm25": 3})],
        )

        build_school_environmental_insights("school-test", devices=[device])

        calls = [call.kwargs for call in mock_history.call_args_list]
        self.assertEqual(len(calls), 3)
        self.assertIn(
            {"interval": "1h", "limit": 48, "metrics": ("co2", "pm25")},
            [
                {
                    "interval": call["interval"],
                    "limit": call["limit"],
                    "metrics": call["metrics"],
                }
                for call in calls
            ],
        )
        self.assertIn(
            {"interval": "day", "limit": 365, "metrics": ("co2", "pm25")},
            [
                {
                    "interval": call["interval"],
                    "limit": call["limit"],
                    "metrics": call["metrics"],
                }
                for call in calls
            ],
        )
        self.assertIn(
            {
                "interval": "1h",
                "limit": 336,
                "metrics": ("temperature", "relative_humidity"),
            },
            [
                {
                    "interval": call["interval"],
                    "limit": call["limit"],
                    "metrics": call["metrics"],
                }
                for call in calls
            ],
        )
