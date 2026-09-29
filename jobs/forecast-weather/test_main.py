import unittest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import main


def forecast_payload():
    return {"hourly": {
        "time": ["2026-09-29T23:00", "2026-09-30T00:00"],
        "temperature_2m": ["23.7", "18.2"],
        "dew_point_2m": [12, 11],
        "relative_humidity_2m": [50, 60],
        "surface_pressure": [1010, 1011],
        "shortwave_radiation": [0, 0],
        "direct_normal_irradiance": [0, 0],
        "diffuse_radiation": [0, 0],
        "wind_direction_10m": [90, 180],
        "wind_speed_10m": [2, 3],
        "weather_code": [0, 61],
        "snow_depth": [0, 0],
        "precipitation": [0, 1.5],
        "cloud_cover": [0, 90],
    }}


class ForecastWeatherTests(unittest.TestCase):
    def test_request_covers_eight_athens_dates_across_year_boundary(self):
        instant = datetime(2026, 12, 31, 22, 50, tzinfo=timezone.utc)
        with patch.object(main, "datetime") as clock, patch.object(
            main, "OPEN_METEO_TIMEZONE", "Europe/Athens"
        ), patch.object(main, "OPEN_METEO_FORECAST_DAYS", 8):
            clock.now.side_effect = lambda tz: instant.astimezone(tz)
            params, url = main.build_forecast_request()

        self.assertEqual(params["start_date"], "2027-01-01")
        self.assertEqual(params["end_date"], "2027-01-08")
        self.assertEqual(params["timezone"], "Europe/Athens")
        self.assertEqual(
            parse_qs(urlsplit(url).query),
            {key: [str(value)] for key, value in params.items()},
        )

    def test_rows_match_each_hours_forecast(self):
        rows = main.build_forecast_rows(forecast_payload(), {})
        self.assertEqual(
            [(row["forecast_timestamp"], row["temperature_2m"], row["weather_code"])
             for row in rows],
            [
                (datetime(2026, 9, 29, 23), Decimal("23.7"), 0),
                (datetime(2026, 9, 30, 0), Decimal("18.2"), 61),
            ],
        )
        for row in rows:
            self.assertIsInstance(row["temperature_2m"], Decimal)
            self.assertIsInstance(row["weather_code"], int)

    def test_missing_and_invalid_values_stay_distinct_from_zero(self):
        data = forecast_payload()
        data["hourly"]["temperature_2m"] = [None, "invalid"]
        data["hourly"]["weather_code"] = [None, "invalid"]
        main.validate_forecast_payload(data)
        rows = main.build_forecast_rows(data, {})

        for row in rows:
            self.assertIsNone(row["temperature_2m"])
            self.assertIsNone(row["weather_code"])
            self.assertEqual(row["snow_depth"], Decimal("0"))
        self.assertEqual(rows[1]["raw_values"]["temperature_2m"], "invalid")

    def test_malformed_forecasts_stop_the_job_before_database_access(self):
        valid = forecast_payload()["hourly"]
        cases = {
            "missing hourly": {},
            "wrong time type": {"hourly": {**valid, "time": "2026-09-29T23:00"}},
            "short variable": {"hourly": {**valid, "temperature_2m": [23.7]}},
        }
        for case, data in cases.items():
            with self.subTest(case=case), patch.object(
                main, "fetch_forecast_json", return_value=data
            ), patch.object(main, "db_connect") as connect:
                with self.assertRaises(ValueError):
                    main.main()
                connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
