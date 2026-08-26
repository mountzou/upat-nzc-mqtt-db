"""Contracts for Open-Meteo-aligned weather column names."""

from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RENAME_MIGRATION = (
    ROOT / "db" / "migrations" / "013_weather_open_meteo_column_names.sql"
)
INIT_SQL = ROOT / "db" / "init.sql"
API_SOURCE = ROOT / "api" / "main.py"
COLLECTOR_SOURCE = ROOT / "weather-collector" / "main.py"

RENAMES = (
    ("temperature_2m_c", "temperature_2m"),
    ("dew_point_2m_c", "dew_point_2m"),
    ("relative_humidity_2m_percent", "relative_humidity_2m"),
    ("surface_pressure_hpa", "surface_pressure"),
    ("shortwave_radiation_w_m2", "shortwave_radiation"),
    ("direct_normal_irradiance_w_m2", "direct_normal_irradiance"),
    ("diffuse_radiation_w_m2", "diffuse_radiation"),
    ("wind_direction_10m_degrees", "wind_direction_10m"),
    ("wind_speed_10m_ms", "wind_speed_10m"),
    ("snow_depth_m", "snow_depth"),
    ("precipitation_mm", "precipitation"),
    ("cloud_cover_percent", "cloud_cover"),
)

OPEN_METEO_COLUMNS = tuple(new_name for _, new_name in RENAMES) + ("weather_code",)
LEGACY_COLUMNS = tuple(old_name for old_name, _ in RENAMES)


def weather_table_block(sql: str) -> str:
    match = re.search(
        r"create\s+table\s+if\s+not\s+exists\s+weather_hourly_forecasts\s*\((.*?)\n\);",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match is None:
        raise AssertionError("weather_hourly_forecasts definition not found")
    return re.sub(r"\s+", " ", match.group(1)).lower()


class WeatherSchemaTests(unittest.TestCase):
    def test_fresh_schema_uses_exact_open_meteo_names(self):
        block = weather_table_block(INIT_SQL.read_text(encoding="utf-8"))
        for column in OPEN_METEO_COLUMNS:
            self.assertRegex(block, rf"\b{column}\s+(numeric|integer)\b")
        for legacy_column in LEGACY_COLUMNS:
            self.assertNotRegex(block, rf"\b{legacy_column}\b")

    def test_rename_migration_is_complete_and_data_preserving(self):
        sql = RENAME_MIGRATION.read_text(encoding="utf-8").lower()
        for old_name, new_name in RENAMES:
            self.assertIn(f"('{old_name}', '{new_name}')", sql)
        for forbidden in ("drop table", "drop column", "truncate", "delete from"):
            self.assertNotIn(forbidden, sql)
        self.assertIn("rename column", sql)

    def test_fresh_database_init_runs_the_idempotent_rename_migration(self):
        self.assertIn(
            r"\ir migrations/013_weather_open_meteo_column_names.sql",
            INIT_SQL.read_text(encoding="utf-8"),
        )

    def test_collector_has_no_open_meteo_to_database_name_mapping(self):
        source = COLLECTOR_SOURCE.read_text(encoding="utf-8")
        self.assertNotIn("DB_COLUMNS_BY_VARIABLE", source)
        self.assertIn("*HOURLY_VARIABLES", source)

    def test_api_preserves_existing_unit_explicit_response_contract(self):
        source = API_SOURCE.read_text(encoding="utf-8").lower()
        for api_name, database_name in RENAMES:
            self.assertIn(f"{database_name} as {api_name}", source)


if __name__ == "__main__":
    unittest.main()
