import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import main
from fastapi import HTTPException


class _FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.query = None
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, params):
        self.query = query
        self.params = params

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


class _FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def cursor(self):
        return self._cursor


class PvActualsTests(unittest.TestCase):
    @patch("main.get_connection")
    def test_returns_bounded_read_only_plant_power(self, mock_get_connection):
        cursor = _FakeCursor(
            [
                {
                    "observed_at": datetime(2026, 9, 2, 9, 0, tzinfo=timezone.utc),
                    "local_date": date(2026, 9, 2),
                    "active_power_kw": Decimal("42.125"),
                    "reactive_power_kvar": Decimal("1.25"),
                    "mppt_power_kw": Decimal("43.5"),
                    "power_factor": Decimal("0.98"),
                    "quality_status": "complete",
                    "source_run_id": "must-not-leak",
                }
            ]
        )
        mock_get_connection.return_value = _FakeConnection(cursor)

        payload = main.get_pv_actuals(
            start_date=date(2026, 9, 1),
            end_date=date(2026, 9, 2),
        )

        self.assertEqual(payload["source_id"], "postgres-pv-plant-readings")
        self.assertEqual(payload["site_key"], "upat-pv")
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["points"][0]["active_power_kw"], 42.125)
        self.assertEqual(payload["points"][0]["reactive_power_kvar"], 1.25)
        self.assertEqual(payload["points"][0]["mppt_power_kw"], 43.5)
        self.assertEqual(payload["points"][0]["power_factor"], 0.98)
        self.assertNotIn("source_run_id", payload["points"][0])
        self.assertEqual(
            cursor.params,
            (
                "upat-pv",
                datetime(2026, 8, 31, 21, 0, tzinfo=timezone.utc),
                datetime(2026, 9, 2, 21, 0, tzinfo=timezone.utc),
            ),
        )
        normalized_query = " ".join(cursor.query.split()).lower()
        self.assertTrue(normalized_query.startswith("select "))
        self.assertNotIn("insert ", normalized_query)
        self.assertNotIn("update ", normalized_query)
        self.assertNotIn("delete ", normalized_query)

    def test_rejects_inverted_range_before_database_access(self):
        with patch("main.get_connection") as mock_get_connection:
            with self.assertRaises(HTTPException) as raised:
                main.get_pv_actuals(
                    start_date=date(2026, 9, 2),
                    end_date=date(2026, 9, 1),
                )

        self.assertEqual(raised.exception.status_code, 400)
        mock_get_connection.assert_not_called()

    @patch("main.get_connection")
    def test_returns_other_pv_metrics_when_active_power_is_null(
        self,
        mock_get_connection,
    ):
        cursor = _FakeCursor(
            [
                {
                    "observed_at": datetime(2026, 9, 2, 9, 0, tzinfo=timezone.utc),
                    "local_date": date(2026, 9, 2),
                    "active_power_kw": None,
                    "reactive_power_kvar": Decimal("1.25"),
                    "mppt_power_kw": Decimal("43.5"),
                    "power_factor": None,
                    "quality_status": "partial",
                }
            ]
        )
        mock_get_connection.return_value = _FakeConnection(cursor)

        payload = main.get_pv_actuals(
            start_date=date(2026, 9, 2),
            end_date=date(2026, 9, 2),
        )

        self.assertIsNone(payload["points"][0]["active_power_kw"])
        self.assertEqual(payload["points"][0]["mppt_power_kw"], 43.5)

    def test_rejects_more_than_ninety_days_before_database_access(self):
        with patch("main.get_connection") as mock_get_connection:
            with self.assertRaises(HTTPException) as raised:
                main.get_pv_actuals(
                    start_date=date(2026, 1, 1),
                    end_date=date(2026, 4, 1),
                )

        self.assertEqual(raised.exception.status_code, 400)
        mock_get_connection.assert_not_called()

    @patch("main.get_connection")
    def test_athens_fallback_day_uses_true_twenty_five_hour_utc_window(
        self,
        mock_get_connection,
    ):
        cursor = _FakeCursor([])
        mock_get_connection.return_value = _FakeConnection(cursor)

        main.get_pv_actuals(
            start_date=date(2026, 10, 25),
            end_date=date(2026, 10, 25),
        )

        self.assertEqual(
            cursor.params,
            (
                "upat-pv",
                datetime(2026, 10, 24, 21, 0, tzinfo=timezone.utc),
                datetime(2026, 10, 25, 22, 0, tzinfo=timezone.utc),
            ),
        )

    @patch("main.get_connection", side_effect=RuntimeError("password=secret"))
    def test_database_failure_is_sanitized(self, _mock_get_connection):
        with self.assertRaises(HTTPException) as raised:
            main.get_pv_actuals(
                start_date=date(2026, 9, 1),
                end_date=date(2026, 9, 2),
            )

        self.assertEqual(raised.exception.status_code, 503)
        self.assertEqual(
            raised.exception.detail,
            "PV actuals are temporarily unavailable",
        )
        self.assertNotIn("secret", str(raised.exception.detail))

    @patch("main.get_connection")
    def test_bounds_returns_read_only_available_local_dates(
        self,
        mock_get_connection,
    ):
        cursor = _FakeCursor(
            [
                {
                    "min_date": date(2026, 5, 7),
                    "max_date": date(2026, 9, 2),
                }
            ]
        )
        mock_get_connection.return_value = _FakeConnection(cursor)

        payload = main.get_pv_actuals_bounds()

        self.assertEqual(payload["min_date"], "2026-05-07")
        self.assertEqual(payload["max_date"], "2026-09-02")
        self.assertEqual(cursor.params, ("upat-pv",))
        normalized_query = " ".join(cursor.query.split()).lower()
        self.assertTrue(normalized_query.startswith("select "))
        self.assertNotIn("insert ", normalized_query)
        self.assertNotIn("update ", normalized_query)
        self.assertNotIn("delete ", normalized_query)

    @patch("main.get_connection")
    def test_bounds_supports_an_empty_postgres_source(self, mock_get_connection):
        cursor = _FakeCursor([{"min_date": None, "max_date": None}])
        mock_get_connection.return_value = _FakeConnection(cursor)

        payload = main.get_pv_actuals_bounds()

        self.assertIsNone(payload["min_date"])
        self.assertIsNone(payload["max_date"])

    def test_route_uses_existing_telemetry_bearer_dependency(self):
        for path in ("/pv/actuals", "/pv/actuals/bounds"):
            with self.subTest(path=path):
                route = next(
                    route
                    for route in main.app.routes
                    if getattr(route, "path", None) == path
                )

                self.assertEqual(
                    [
                        dependency.call
                        for dependency in route.dependant.dependencies
                    ],
                    [main.require_ops_telemetry_token],
                )


if __name__ == "__main__":
    unittest.main()
