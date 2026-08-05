import unittest
from datetime import date, datetime, timezone
from unittest.mock import patch

import main


class _FakeCursor:
    def __init__(self, run, room_rows):
        self.run = run
        self.room_rows = room_rows
        self.execute_count = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, _query, _params):
        self.execute_count += 1

    def fetchone(self):
        return self.run

    def fetchall(self):
        return self.room_rows


class _FakeConnection:
    def __init__(self, cursor):
        self.cursor_instance = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def cursor(self):
        return self.cursor_instance


def _run(hourly_load):
    now = datetime(2026, 8, 5, 6, tzinfo=timezone.utc)
    return {
        "id": 1,
        "school_id": "school_10",
        "recording_date": date(2026, 8, 5),
        "request_url": "https://example.test/simulate/day-ahead",
        "request_path": "/simulate/day-ahead",
        "request_body": {"school_id": "school_10", "target_date": "2026-08-06"},
        "http_status": 200,
        "status": "success",
        "simulation_engine": "energyplus",
        "external_run_id": "run-1",
        "day_ahead_date": date(2026, 8, 6),
        "requested_rooms": 1,
        "successful_rooms": 1,
        "failed_rooms": 0,
        "facility_kwh": 24,
        "equipment_kwh": 0,
        "lighting_kwh": 0,
        "heating_liters": 0,
        "cooling_kwh": 0,
        "fans_hvac_kwh": 0,
        "response_json": {"hourly_load": hourly_load} if hourly_load else {},
        "started_at": now,
        "completed_at": now,
        "created_at": now,
        "updated_at": now,
    }


class DayAheadApiContractTests(unittest.TestCase):
    @patch("main.get_connection")
    def test_latest_endpoint_republishes_persisted_hourly_load(self, get_connection):
        hourly_load = {
            "complete": True,
            "expected_intervals": 24,
            "items": [{"predicted_energy_kwh": 1.0}] * 24,
        }
        cursor = _FakeCursor(_run(hourly_load), [])
        get_connection.return_value = _FakeConnection(cursor)

        response = main.get_latest_day_ahead_simulation_results("school_10")

        self.assertEqual(response["day_ahead_date"], date(2026, 8, 6))
        self.assertEqual(response["hourly_load"], hourly_load)
        self.assertEqual(cursor.execute_count, 2)

    @patch("main.get_connection")
    def test_legacy_run_without_hourly_profile_remains_readable(self, get_connection):
        get_connection.return_value = _FakeConnection(_FakeCursor(_run(None), []))

        response = main.get_latest_day_ahead_simulation_results("school_10")

        self.assertIsNone(response["hourly_load"])


if __name__ == "__main__":
    unittest.main()
