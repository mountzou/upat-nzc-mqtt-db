import os
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from fastapi import HTTPException
from psycopg2 import sql

import main


class _FakeCursor:
    def __init__(self, row):
        self.row = row
        self.query = None
        self.execute_count = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query):
        self.query = query
        self.execute_count += 1

    def fetchone(self):
        return self.row


class _FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def cursor(self):
        return self._cursor


class OperationsTelemetryTests(unittest.TestCase):
    @patch("main.get_runtime_metrics")
    @patch("main.get_connection")
    def test_returns_one_index_friendly_sanitized_aggregate(
        self,
        mock_get_connection,
        mock_runtime_metrics,
    ):
        cursor = _FakeCursor(
            {
                "total_devices": 8,
                "live_devices": 6,
                "stale_devices": 1,
                "offline_devices": 1,
                "upat_devices": 5,
                "shelly_devices": 3,
                "latest_reading_at": datetime(2026, 8, 3, 12, 0),
                "database_size_bytes": 157286400,
                "active_connections": 4,
                "max_connections": 100,
                "device_id": "must-not-leak",
                "password": "must-not-leak",
            }
        )
        mock_get_connection.return_value = _FakeConnection(cursor)
        mock_runtime_metrics.return_value = {
            "cpu_load_1m_percent": 22.5,
            "memory_used_percent": 48.0,
            "disk_used_percent": 61.0,
            "uptime_seconds": 86400,
            "hostname": "must-not-leak",
        }

        result = main.fetch_operational_telemetry()

        self.assertEqual(result["fleet"]["total_devices"], 8)
        self.assertEqual(result["fleet"]["availability_percent"], 75.0)
        self.assertEqual(result["database"]["connections_used_percent"], 4.0)
        self.assertEqual(
            result["fleet"]["live_devices"]
            + result["fleet"]["stale_devices"]
            + result["fleet"]["offline_devices"],
            result["fleet"]["total_devices"],
        )
        self.assertEqual(cursor.execute_count, 1)
        self.assertIn("LEFT JOIN LATERAL", cursor.query)
        self.assertIn("upat_raw_messages", cursor.query)
        self.assertIn("shelly_raw_messages", cursor.query)
        self.assertIn("CURRENT_TIMESTAMP AT TIME ZONE 'UTC'", cursor.query)
        self.assertNotIn("payload", cursor.query.lower())
        self.assertNotIn("device_id", str(result))
        self.assertNotIn("must-not-leak", str(result))
        self.assertEqual(
            set(result),
            {"generated_at", "fleet", "infrastructure", "database"},
        )
        self.assertEqual(
            set(result["fleet"]),
            {
                "total_devices",
                "live_devices",
                "stale_devices",
                "offline_devices",
                "upat_devices",
                "shelly_devices",
                "availability_percent",
                "latest_reading_at",
                "live_threshold_minutes",
                "offline_threshold_hours",
            },
        )
        self.assertEqual(
            set(result["infrastructure"]),
            {
                "cpu_load_1m_percent",
                "memory_used_percent",
                "disk_used_percent",
                "uptime_seconds",
            },
        )
        self.assertEqual(
            set(result["database"]),
            {
                "size_bytes",
                "active_connections",
                "max_connections",
                "connections_used_percent",
            },
        )
        self.assertTrue(result["generated_at"].endswith("Z"))

    @patch("main.get_runtime_metrics")
    @patch("main.get_connection")
    def test_empty_fleet_returns_null_availability(
        self,
        mock_get_connection,
        mock_runtime_metrics,
    ):
        cursor = _FakeCursor(
            {
                "total_devices": 0,
                "live_devices": 0,
                "stale_devices": 0,
                "offline_devices": 0,
                "upat_devices": 0,
                "shelly_devices": 0,
                "latest_reading_at": None,
                "database_size_bytes": 0,
                "active_connections": 0,
                "max_connections": 100,
            }
        )
        mock_get_connection.return_value = _FakeConnection(cursor)
        mock_runtime_metrics.return_value = {}

        result = main.fetch_operational_telemetry()

        self.assertEqual(result["fleet"]["total_devices"], 0)
        self.assertEqual(result["fleet"]["offline_devices"], 0)
        self.assertIsNone(result["fleet"]["availability_percent"])
        self.assertIsNone(result["fleet"]["latest_reading_at"])
        self.assertTrue(
            all(value is None for value in result["infrastructure"].values())
        )

    @patch("main._read_runtime_memory", return_value=(None, None))
    @patch("main.shutil.disk_usage", side_effect=OSError("unavailable"))
    @patch("main.os.getloadavg", side_effect=OSError("unavailable"))
    @patch("builtins.open", side_effect=OSError("unavailable"))
    def test_optional_runtime_counter_failures_return_nulls(
        self,
        _mock_open,
        _mock_getloadavg,
        _mock_disk_usage,
        _mock_read_memory,
    ):
        result = main.get_runtime_metrics()

        self.assertEqual(
            result,
            {
                "cpu_load_1m_percent": None,
                "memory_used_percent": None,
                "disk_used_percent": None,
                "uptime_seconds": None,
            },
        )

    @patch("main.fetch_operational_telemetry")
    def test_route_hides_internal_exception_details(self, mock_fetch):
        mock_fetch.side_effect = RuntimeError("password=do-not-expose")
        with self.assertRaises(HTTPException) as context:
            main.operational_telemetry()

        self.assertEqual(context.exception.status_code, 503)
        self.assertEqual(
            context.exception.detail,
            "Operational telemetry is temporarily unavailable",
        )
        self.assertNotIn("do-not-expose", str(context.exception.detail))


@unittest.skipUnless(
    os.getenv("OPERATIONS_RUN_DB_TESTS") == "1",
    "set OPERATIONS_RUN_DB_TESTS=1 against a disposable PostgreSQL database",
)
class OperationsTelemetryDatabaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema_name = f"operations_telemetry_test_{uuid.uuid4().hex}"
        cls.original_get_connection = main.get_connection

        with cls.original_get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("CREATE SCHEMA {}").format(
                        sql.Identifier(cls.schema_name)
                    )
                )

        def get_test_connection():
            conn = cls.original_get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SET search_path TO {}").format(
                        sql.Identifier(cls.schema_name)
                    )
                )
            return conn

        main.get_connection = get_test_connection

        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE upat_devices (
                        source TEXT NOT NULL,
                        device_id TEXT NOT NULL,
                        UNIQUE (source, device_id)
                    );
                    CREATE TABLE shelly_devices (
                        source TEXT NOT NULL,
                        device_id TEXT NOT NULL,
                        UNIQUE (source, device_id)
                    );
                    CREATE TABLE upat_raw_messages (
                        device_id TEXT NOT NULL,
                        event_time TIMESTAMP
                    );
                    CREATE INDEX ON upat_raw_messages (
                        device_id,
                        event_time DESC
                    );
                    CREATE TABLE shelly_raw_messages (
                        device_id TEXT NOT NULL,
                        event_time TIMESTAMP
                    );
                    CREATE INDEX ON shelly_raw_messages (
                        device_id,
                        event_time DESC
                    );
                """)

    @classmethod
    def tearDownClass(cls):
        main.get_connection = cls.original_get_connection

        with cls.original_get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(
                        sql.Identifier(cls.schema_name)
                    )
                )

    def test_fleet_categories_are_calculated_by_postgresql(self):
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)

        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO upat_devices (source, device_id) VALUES (%s, %s)",
                    [("ttn", f"upat-{index}") for index in range(1, 6)],
                )
                cur.executemany(
                    "INSERT INTO shelly_devices (source, device_id) VALUES (%s, %s)",
                    [("shelly", f"shelly-{index}") for index in range(1, 4)],
                )
                cur.executemany(
                    """
                    INSERT INTO upat_raw_messages (device_id, event_time)
                    VALUES (%s, %s)
                    """,
                    [
                        (f"upat-{index}", now_utc - timedelta(minutes=5))
                        for index in range(1, 5)
                    ]
                    + [("upat-5", now_utc - timedelta(hours=2))],
                )
                cur.executemany(
                    """
                    INSERT INTO shelly_raw_messages (device_id, event_time)
                    VALUES (%s, %s)
                    """,
                    [
                        (f"shelly-{index}", now_utc - timedelta(minutes=5))
                        for index in range(1, 3)
                    ],
                )

        with patch("main.get_runtime_metrics", return_value={}):
            result = main.fetch_operational_telemetry()

        self.assertEqual(result["fleet"]["total_devices"], 8)
        self.assertEqual(result["fleet"]["live_devices"], 6)
        self.assertEqual(result["fleet"]["stale_devices"], 1)
        self.assertEqual(result["fleet"]["offline_devices"], 1)
        self.assertEqual(result["fleet"]["upat_devices"], 5)
        self.assertEqual(result["fleet"]["shelly_devices"], 3)
        self.assertEqual(result["fleet"]["availability_percent"], 75.0)


if __name__ == "__main__":
    unittest.main()
