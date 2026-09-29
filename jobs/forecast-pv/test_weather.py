import io
import unittest
from contextlib import redirect_stderr
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import main as forecast
import pandas as pd
import psycopg2


class WeatherDatabaseTests(unittest.TestCase):
    def setUp(self):
        # UTC still says December 31, but tomorrow in Athens is January 2.
        self.now = datetime(2026, 12, 31, 22, 50, tzinfo=timezone.utc)
        self.rows = [
            [ts, 20, 700, 800, 100, 10, 7.2, self.now - timedelta(minutes=10)]
            for ts in pd.date_range("2027-01-02", periods=24, freq="h")
        ]
        self.connection = MagicMock()
        self.cursor = self.connection.cursor.return_value.__enter__.return_value
        self.cursor.fetchall.side_effect = lambda: self.rows
        for patcher in (
            patch.object(forecast, "db_connect", return_value=self.connection),
            patch.object(forecast, "WEATHER_TIMEZONE", "Europe/Athens"),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        clock = patch.object(forecast, "datetime")
        clock.start().now.return_value = self.now
        self.addCleanup(clock.stop)

    def test_selects_tomorrow_in_athens_and_passes_all_hours_to_real_model(self):
        self.rows[-1][7] = self.now - timedelta(minutes=5)
        result = forecast.run_forecast()
        self.assertEqual(self.cursor.execute.call_args.args[1], (
            "Europe/Athens", datetime(2027, 1, 2).date(),
        ))
        self.assertEqual(result["time"].tolist(), [row[0] for row in self.rows])
        self.assertTrue(result["predicted_power_kw"].ge(0).all())
        self.connection.close.assert_called_once()

    def test_bad_weather_stops_before_loading_model(self):
        valid_rows = self.rows
        cases = {
            "missing hour": valid_rows[:-1],
            "duplicate hour": [valid_rows[1], *valid_rows[1:]],
            "stale": [row[:7] + [self.now - timedelta(hours=25)] for row in valid_rows],
            "null value": [valid_rows[0][:1] + [None] + valid_rows[0][2:], *valid_rows[1:]],
            "infinite value": [valid_rows[0][:1] + [float("inf")] + valid_rows[0][2:], *valid_rows[1:]],
        }
        with patch.object(forecast.joblib, "load") as model_load:
            for label, rows in cases.items():
                with self.subTest(label=label), self.assertRaises(ValueError):
                    self.rows = rows
                    forecast.run_forecast()
            model_load.assert_not_called()

    def test_database_error_exits_without_saving(self):
        with (
            patch.object(forecast, "db_connect", side_effect=psycopg2.OperationalError("unavailable")),
            patch.object(forecast, "save_forecast_to_db") as save,
            patch("sys.argv", ["forecast-pv"]),
            redirect_stderr(io.StringIO()),
        ):
            self.assertEqual(forecast.main(), 1)
        save.assert_not_called()


if __name__ == "__main__":
    unittest.main()
