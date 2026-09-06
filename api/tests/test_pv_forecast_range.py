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


class _FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def cursor(self):
        return self._cursor


class PvForecastRangeTests(unittest.TestCase):
    @patch("main.get_connection")
    def test_returns_latest_postgres_forecast_per_requested_day(
        self,
        mock_get_connection,
    ):
        cursor = _FakeCursor(
            [
                {
                    "forecast_date": date(2026, 9, 3),
                    "forecast_timestamp": datetime(
                        2026,
                        9,
                        2,
                        21,
                        tzinfo=timezone.utc,
                    ),
                    "forecast_hour": 0,
                    "predicted_power_kw": Decimal("0.0"),
                },
                {
                    "forecast_date": date(2026, 9, 3),
                    "forecast_timestamp": datetime(
                        2026,
                        9,
                        3,
                        9,
                        tzinfo=timezone.utc,
                    ),
                    "forecast_hour": 12,
                    "predicted_power_kw": Decimal("42.5"),
                },
                {
                    "forecast_date": date(2026, 9, 4),
                    "forecast_timestamp": datetime(
                        2026,
                        9,
                        3,
                        21,
                        tzinfo=timezone.utc,
                    ),
                    "forecast_hour": 0,
                    "predicted_power_kw": Decimal("0.0"),
                },
            ]
        )
        mock_get_connection.return_value = _FakeConnection(cursor)

        payload = main.get_pv_day_ahead_forecast_range(
            start_date=date(2026, 9, 3),
            end_date=date(2026, 9, 4),
        )

        self.assertEqual(payload["source_id"], "postgres-pv-day-ahead-forecasts")
        self.assertEqual(payload["timezone"], "Europe/Athens")
        self.assertEqual(payload["count"], 2)
        self.assertEqual(payload["forecasts"][0]["forecast_date"], "2026-09-03")
        self.assertEqual(payload["forecasts"][0]["count"], 2)
        self.assertEqual(
            payload["forecasts"][0]["items"][1]["predicted_power_kw"],
            42.5,
        )
        self.assertEqual(cursor.params, (date(2026, 9, 3), date(2026, 9, 4)))
        normalized_query = " ".join(cursor.query.split()).lower()
        self.assertTrue(normalized_query.startswith("with latest_runs as"))
        self.assertNotIn("insert ", normalized_query)
        self.assertNotIn("update ", normalized_query)
        self.assertNotIn("delete ", normalized_query)

    def test_rejects_inverted_range_before_database_access(self):
        with patch("main.get_connection") as mock_get_connection:
            with self.assertRaises(HTTPException) as raised:
                main.get_pv_day_ahead_forecast_range(
                    start_date=date(2026, 9, 4),
                    end_date=date(2026, 9, 3),
                )

        self.assertEqual(raised.exception.status_code, 400)
        mock_get_connection.assert_not_called()

    @patch("main.get_connection", side_effect=RuntimeError("password=secret"))
    def test_database_failure_is_sanitized(self, _mock_get_connection):
        with self.assertRaises(HTTPException) as raised:
            main.get_pv_day_ahead_forecast_range(
                start_date=date(2026, 9, 3),
                end_date=date(2026, 9, 4),
            )

        self.assertEqual(raised.exception.status_code, 503)
        self.assertEqual(
            raised.exception.detail,
            "PV forecasts are temporarily unavailable",
        )

    def test_route_uses_existing_telemetry_bearer_dependency(self):
        route = next(
            route
            for route in main.app.routes
            if getattr(route, "path", None) == "/pv/day-ahead/range"
        )

        self.assertEqual(
            [dependency.call for dependency in route.dependant.dependencies],
            [main.require_ops_telemetry_token],
        )


if __name__ == "__main__":
    unittest.main()
