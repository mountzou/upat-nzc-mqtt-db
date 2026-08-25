"""Static safety contracts for the additive PV actual-telemetry schema."""

from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "db" / "migrations" / "010_pv_actual_telemetry.sql"
INIT_SQL = ROOT / "db" / "init.sql"

EXPECTED_TABLES = {
    "pv_plants",
    "pv_devices",
    "pv_ingestion_runs",
    "pv_source_state",
    "pv_device_readings_5m",
    "pv_plant_readings_5m",
}


class PvActualTelemetrySchemaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = MIGRATION.read_text(encoding="utf-8")
        cls.normalized = re.sub(r"\s+", " ", cls.sql).lower()

    def test_migration_is_additive_only(self):
        for forbidden in (
            " alter table ",
            " drop table ",
            " drop index ",
            " truncate ",
            " delete from ",
            " create extension ",
        ):
            self.assertNotIn(forbidden, f" {self.normalized} ")

    def test_creates_exactly_the_six_v1_tables_idempotently(self):
        created = set(
            re.findall(
                r"create\s+table\s+if\s+not\s+exists\s+([a-z0-9_]+)",
                self.sql,
                flags=re.IGNORECASE,
            )
        )
        self.assertEqual(EXPECTED_TABLES, created)

    def test_reading_keys_are_idempotent_and_timezone_aware(self):
        self.assertIn(
            "primary key (device_id, observed_at)",
            self.normalized,
        )
        self.assertIn(
            "primary key (plant_id, observed_at)",
            self.normalized,
        )
        self.assertGreaterEqual(self.normalized.count("timestamptz"), 20)

    def test_retains_extra_kpis_without_jsonb_index(self):
        self.assertIn("extra_kpis jsonb", self.normalized)
        self.assertNotRegex(self.normalized, r"create\s+index[^;]+extra_kpis")
        self.assertNotIn(" using gin ", f" {self.normalized} ")

    def test_live_pipeline_source_contract_is_accepted(self):
        self.assertIn("'fusion_live'", self.normalized)
        self.assertIn("'fusion_live_device_derived'", self.normalized)
        self.assertIn("provider_collect_time_ms bigint", self.normalized)

    def test_does_not_modify_existing_forecast_tables(self):
        for table in (
            "pv_day_ahead_forecast_runs",
            "pv_day_ahead_forecast_hourly",
            "pv_shadow_forecast_runs",
            "pv_shadow_forecast_hourly",
        ):
            self.assertNotIn(table, self.normalized)

    def test_fresh_database_init_includes_the_migration(self):
        init_sql = INIT_SQL.read_text(encoding="utf-8")
        self.assertIn(
            r"\ir migrations/010_pv_actual_telemetry.sql",
            init_sql,
        )


if __name__ == "__main__":
    unittest.main()
