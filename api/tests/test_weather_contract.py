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

        normalized_query = " ".join(cursor.query.lower().split())
        for fragment in (
            "temperature_2m as temperature_2m_c",
            "relative_humidity_2m as relative_humidity_2m_percent",
            "shortwave_radiation as shortwave_radiation_w_m2",
            "wind_speed_10m as wind_speed_10m_ms",
            "cloud_cover as cloud_cover_percent",
        ):
            self.assertIn(fragment, normalized_query)


if __name__ == "__main__":
    unittest.main()
