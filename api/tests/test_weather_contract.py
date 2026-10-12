import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import patch

from fastapi.testclient import TestClient

import main

PATH = "/internal/data/weather/hourly/forecast"
SERVICE_TOKEN = "weather-contract-" + "w" * 40

EXPECTED_VALUES = {
    "temperature_2m_c": 23.7,
    "dew_point_2m_c": None,
    "relative_humidity_2m_percent": 50.0,
    "surface_pressure_hpa": 1010.5,
    "shortwave_radiation_w_m2": 450.25,
    "direct_normal_irradiance_w_m2": 800.5,
    "diffuse_radiation_w_m2": 75.25,
    "wind_direction_10m_degrees": 90.0,
    "wind_speed_10m_ms": 2.5,
    "weather_code": 2,
    "snow_depth_m": 0.0,
    "precipitation_mm": 0.0,
    "cloud_cover_percent": 25.0,
}


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
    row.update({
        field: Decimal(str(value)) if value is not None and field != "weather_code" else value
        for field, value in EXPECTED_VALUES.items()
    })
    return row


def _cursor(get_connection, rows):
    connection = get_connection.return_value.__enter__.return_value
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = rows
    return cursor


class WeatherApiContractTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch.dict("os.environ", {"DATA_SERVICE_TOKEN": SERVICE_TOKEN}))
        self.client = self.enterContext(TestClient(main.app))
        self.headers = {"Authorization": "Bearer " + SERVICE_TOKEN}

    @patch("main.get_connection")
    def test_forecast_http_contract_preserves_fields_values_and_types(self, get_connection):
        cursor = _cursor(get_connection, [_weather_row()])
        response = self.client.get(
            PATH, params={"start": "2026-08-26", "end": "2026-08-26"},
            headers=self.headers,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {
            "start": "2026-08-26T00:00:00",
            "end": "2026-08-26T23:59:59.999999",
            "count": 1,
            "items": [{
                "timestamp": "2026-08-26T12:00:00",
                "date": "2026-08-26",
                "hour": 12,
                "values": EXPECTED_VALUES,
                "source": "open-meteo",
                "latitude": 37.068,
                "longitude": 22.026,
                "timezone": "Europe/Athens",
                "fetched_at": "2026-08-25T19:50:00+00:00",
            }],
        })
        item = response.json()["items"][0]
        for field, expected in EXPECTED_VALUES.items():
            self.assertIs(type(item["values"][field]), type(expected), field)
        self.assertIs(type(item["latitude"]), float)
        self.assertIs(type(item["longitude"]), float)
        self.assertIs(type(item["hour"]), int)

        cursor.execute.assert_called_once()
        query, params = cursor.execute.call_args.args
        self.assertEqual(params, (
            datetime(2026, 8, 26),
            datetime(2026, 8, 26, 23, 59, 59, 999999),
        ))
        normalized_query = " ".join(query.lower().split())
        for fragment in (
            "temperature_2m as temperature_2m_c",
            "relative_humidity_2m as relative_humidity_2m_percent",
            "shortwave_radiation as shortwave_radiation_w_m2",
            "wind_speed_10m as wind_speed_10m_ms",
            "cloud_cover as cloud_cover_percent",
        ):
            self.assertIn(fragment, normalized_query)

    @patch("main.get_connection")
    def test_specific_time_bounds_and_empty_forecast(self, get_connection):
        cursor = _cursor(get_connection, [])
        response = self.client.get(
            PATH, params={"start": "2026-08-26T10:30", "end": "2026-08-26T12:00"},
            headers=self.headers,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {
            "start": "2026-08-26T10:30:00",
            "end": "2026-08-26T12:00:00",
            "count": 0,
            "items": [],
        })
        cursor.execute.assert_called_once()
        self.assertEqual(cursor.execute.call_args.args[1], (
            datetime(2026, 8, 26, 10, 30), datetime(2026, 8, 26, 12),
        ))

    @patch("main.get_connection")
    def test_invalid_bounds_are_rejected_before_database_access(self, get_connection):
        cases = [
            {"start": "2026-08-26"},
            {"end": "2026-08-26"},
            {"start": "invalid", "end": "2026-08-26"},
            {"start": "2026-08-27", "end": "2026-08-26"},
        ]
        for params in cases:
            with self.subTest(params=params):
                response = self.client.get(PATH, params=params, headers=self.headers)
                self.assertEqual(response.status_code, 400)
        get_connection.assert_not_called()


if __name__ == "__main__":
    unittest.main()
