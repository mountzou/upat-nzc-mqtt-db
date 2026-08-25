import copy
import json
import math
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from pipeline import (
    PipelineValidationError,
    build_ingestion_batch,
    build_request_window,
)


FIXTURE_PATH = Path(__file__).parent / "tests" / "fixtures" / "fusionsolar_sample.json"


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
        self.window = build_request_window(
            target_date=date.fromisoformat(self.fixture["target_date"]),
            lookback_days=1,
        )

    def build_batch(self, fixture=None):
        data = fixture or self.fixture
        return build_ingestion_batch(
            plant_code=data["plant_code"],
            request_window=self.window,
            device_list_payload=data["device_list"],
            history_by_device_type={
                int(key): value
                for key, value in data["history_by_device_type"].items()
            },
            source_kind="fixture",
            api_calls=data["api_calls"],
            collected_at=datetime(2026, 8, 25, tzinfo=timezone.utc),
        )

    def test_builds_persistence_ready_batch_without_storage(self):
        batch = self.build_batch()

        self.assertEqual("pv-ingestion-batch-v1", batch["schema_version"])
        self.assertEqual("fixture", batch["source_kind"])
        self.assertTrue(batch["run_key"].startswith("fixture:"))
        self.assertTrue(
            all(
                row["source_kind"] == "fixture"
                for row in batch["device_readings"]
            )
        )
        self.assertEqual("not_attempted", batch["persistence"]["status"])
        self.assertEqual(0, batch["persistence"]["rows_written"])
        self.assertEqual(4, len(batch["devices"]))
        self.assertEqual(7, len(batch["device_readings"]))
        self.assertEqual(3, len(batch["plant_readings"]))
        self.assertEqual(1, batch["quality"]["partial_plant_timestamp_count"])

    def test_preserves_typed_and_extra_kpis_without_sensitive_device_fields(self):
        batch = self.build_batch()
        first = next(
            row
            for row in batch["device_readings"]
            if row["provider_device_id"] == "101"
        )

        self.assertEqual(10.0, first["active_power_kw"])
        self.assertEqual(40.0, first["temperature_c"])
        self.assertEqual(500.0, first["extra_kpis"]["pv1_u"])
        self.assertNotIn("unused_null", first["extra_kpis"])
        self.assertNotIn("temperature", first["extra_kpis"])
        rendered = json.dumps(batch)
        self.assertNotIn("must-not-leak", rendered)
        self.assertNotIn("longitude", rendered)
        meter = next(
            row
            for row in batch["device_readings"]
            if row["provider_device_id"] == "201"
        )
        self.assertEqual(30.0, meter["extra_kpis"]["reverse_active_cap"])

    def test_plant_aggregation_recomputes_power_factor_and_marks_partial_rows(self):
        batch = self.build_batch()
        first, second, third = batch["plant_readings"]

        self.assertEqual(30.0, first["active_power_kw"])
        self.assertEqual(3.0, first["reactive_power_kvar"])
        self.assertAlmostEqual(30 / math.hypot(30, 3), first["power_factor"])
        self.assertEqual("complete", first["quality_status"])
        self.assertEqual("complete", second["quality_status"])
        self.assertEqual("partial", third["quality_status"])
        self.assertEqual(["101"], third["missing_device_ids"])

    def test_missing_expected_inverter_fails_closed(self):
        fixture = copy.deepcopy(self.fixture)
        fixture["history_by_device_type"]["1"]["data"] = [
            row
            for row in fixture["history_by_device_type"]["1"]["data"]
            if row["devId"] != 101
        ]

        with self.assertRaisesRegex(
            PipelineValidationError,
            "expected inverter",
        ):
            self.build_batch(fixture)

    def test_duplicate_device_timestamp_fails_closed(self):
        fixture = copy.deepcopy(self.fixture)
        fixture["history_by_device_type"]["1"]["data"].append(
            copy.deepcopy(fixture["history_by_device_type"]["1"]["data"][0])
        )

        with self.assertRaisesRegex(PipelineValidationError, "duplicate history"):
            self.build_batch(fixture)

    def test_request_window_is_dst_aware(self):
        window = build_request_window(
            target_date=date(2026, 10, 25),
            lookback_days=3,
        )

        self.assertTrue(window["start_local"].endswith("+03:00"))
        self.assertTrue(window["end_exclusive_local"].endswith("+02:00"))
        self.assertEqual(73 * 60 * 60 * 1000 - 1, window["end_ms"] - window["start_ms"])


if __name__ == "__main__":
    unittest.main()
