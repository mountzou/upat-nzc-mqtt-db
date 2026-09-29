import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import main


class _FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.query = ""
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
        self.cursor_instance = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def cursor(self):
        return self.cursor_instance


def _weather_row():
    row = {
        "source": "open-meteo",
        "latitude": Decimal("37.068"),
        "longitude": Decimal("22.026"),
        "timezone": "Europe/Athens",
        "forecast_timestamp": datetime(2026, 8, 26, 12),
        "forecast_instant": datetime(2026, 8, 26, 9, tzinfo=timezone.utc),
        "forecast_date": date(2026, 8, 26),
        "forecast_hour": 12,
        "fetched_at": datetime(2026, 8, 25, 19, 50, tzinfo=timezone.utc),
    }
    row.update({field: Decimal("1.5") for field in main.WEATHER_FIELDS})
    row["weather_code"] = 2
    return row


class WeatherApiContractTests(unittest.TestCase):
    @patch("main.get_connection")
    def test_forecast_query_aliases_database_names_to_existing_api_names(
        self,
        get_connection,
    ):
        cursor = _FakeCursor([_weather_row()])
        get_connection.return_value = _FakeConnection(cursor)

        response = main.get_weather_hourly_forecast("2026-08-26", "2026-08-26")

        self.assertEqual(1, response["count"])
        self.assertEqual(set(main.WEATHER_FIELDS), set(response["items"][0]["values"]))
        self.assertEqual(
            datetime(2026, 8, 26, 9, tzinfo=timezone.utc),
            response["items"][0]["instant"],
        )

        normalized_query = " ".join(cursor.query.lower().split())
        for fragment in (
            "temperature_2m as temperature_2m_c",
            "relative_humidity_2m as relative_humidity_2m_percent",
            "shortwave_radiation as shortwave_radiation_w_m2",
            "wind_speed_10m as wind_speed_10m_ms",
            "cloud_cover as cloud_cover_percent",
            "order by forecast_instant asc",
        ):
            self.assertIn(fragment, normalized_query)

    @patch("main.get_connection")
    def test_latest_weather_hour_uses_utc_instant_not_ambiguous_local_time(
        self,
        get_connection,
    ):
        cursor = _FakeCursor([_weather_row()])
        get_connection.return_value = _FakeConnection(cursor)

        response = main.get_latest_weather_forecast_hour()

        self.assertEqual(response["instant"], _weather_row()["forecast_instant"])
        self.assertIn("where forecast_instant >= %s", " ".join(cursor.query.lower().split()))
        self.assertEqual(cursor.params[0].tzinfo, timezone.utc)
        self.assertEqual((cursor.params[0].minute, cursor.params[0].second), (0, 0))


if __name__ == "__main__":
    unittest.main()
