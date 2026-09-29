import unittest
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch
from zoneinfo import ZoneInfo

import main


class WeatherCollectorHorizonTests(unittest.TestCase):
    def test_default_forecast_window_has_one_buffer_day(self):
        self.assertEqual(main.OPEN_METEO_FORECAST_DAYS, 8)

    @patch.object(main, "datetime")
    def test_get_forecast_dates_span_eight_inclusive_dates(self, mocked_datetime):
        mocked_datetime.now.return_value = datetime(2026, 8, 8, 22, 50)

        start_date, end_date = main.get_forecast_dates()

        self.assertEqual(start_date, date(2026, 8, 8))
        self.assertEqual(end_date, date(2026, 8, 15))
        self.assertEqual((end_date - start_date).days + 1, 8)

    @patch.object(main, "get_forecast_dates")
    def test_request_uses_explicit_eight_day_bounds(self, get_forecast_dates):
        get_forecast_dates.return_value = (
            date(2026, 8, 8),
            date(2026, 8, 15),
        )

        params, url = main.build_forecast_request()

        self.assertEqual(params["start_date"], "2026-08-08")
        self.assertEqual(params["end_date"], "2026-08-15")
        self.assertEqual(params["timeformat"], "unixtime")
        self.assertIn("start_date=2026-08-08", url)
        self.assertIn("end_date=2026-08-15", url)

    def test_previous_night_snapshot_covers_next_day_seven_day_request(self):
        collector_start = date(2026, 8, 8)
        collector_end = collector_start + timedelta(
            days=main.OPEN_METEO_FORECAST_DAYS - 1
        )
        simulation_start = collector_start + timedelta(days=1)
        simulation_end = simulation_start + timedelta(days=6)

        self.assertLessEqual(collector_start, simulation_start)
        self.assertGreaterEqual(collector_end, simulation_end)


class WeatherCollectorColumnContractTests(unittest.TestCase):
    def test_rejects_mismatched_response_timezone(self):
        with self.assertRaisesRegex(ValueError, "timezone does not match"):
            main.validate_hourly_payload({"timezone": "UTC", "hourly": {}})

    def test_rejects_hourly_variable_that_is_not_an_array(self):
        hourly = {"time": ["2026-08-26T12:00"]}
        for variable in main.HOURLY_VARIABLES:
            hourly[variable] = [1]
        hourly["temperature_2m"] = "1.5"

        with self.assertRaisesRegex(
            ValueError,
            r"hourly\.temperature_2m must be an array",
        ):
            main.validate_hourly_payload({"hourly": hourly})

    def test_rows_use_exact_open_meteo_variable_names(self):
        timestamp = int(datetime(2026, 8, 26, 9, tzinfo=timezone.utc).timestamp())
        hourly = {"time": [timestamp]}
        for variable in main.HOURLY_VARIABLES:
            hourly[variable] = [7 if variable == "weather_code" else "1.5"]

        row = next(
            main.iter_hourly_rows(
                {"hourly": hourly},
                {"start_date": "2026-08-26", "end_date": "2026-09-02"},
            )
        )

        for variable in main.HOURLY_VARIABLES:
            self.assertIn(variable, row)
        self.assertEqual(7, row["weather_code"])
        self.assertEqual(Decimal("1.5"), row["temperature_2m"])
        self.assertEqual(datetime(2026, 8, 26, 12), row["forecast_timestamp"])
        self.assertEqual(datetime(2026, 8, 26, 9, tzinfo=timezone.utc), row["forecast_instant"])

        for legacy_column in (
            "temperature_2m_c",
            "relative_humidity_2m_percent",
            "shortwave_radiation_w_m2",
            "wind_speed_10m_ms",
        ):
            self.assertNotIn(legacy_column, row)

    def test_repeated_athens_hour_keeps_two_distinct_instants(self):
        instants = [datetime(2026, 10, 25, hour, tzinfo=timezone.utc) for hour in (0, 1)]
        hourly = {"time": [int(instant.timestamp()) for instant in instants]}
        for variable in main.HOURLY_VARIABLES:
            hourly[variable] = [7, 7]
        rows = list(main.iter_hourly_rows({"hourly": hourly}, {}))

        self.assertEqual([row["forecast_timestamp"] for row in rows],
                         [datetime(2026, 10, 25, 3)] * 2)
        self.assertEqual([row["forecast_instant"] for row in rows], instants)

    def test_rejects_local_clock_strings_without_unique_instants(self):
        hourly = {"time": ["2026-10-25T03:00"]}
        for variable in main.HOURLY_VARIABLES:
            hourly[variable] = [7]
        with self.assertRaisesRegex(ValueError, "UNIX epoch seconds"):
            list(main.iter_hourly_rows({"hourly": hourly}, {}))

    def test_calendar_day_coverage_requires_23_or_25_real_hours(self):
        zone = ZoneInfo("Europe/Athens")
        for target_date, expected_hours in ((date(2026, 3, 29), 23), (date(2026, 10, 25), 25)):
            start = datetime.combine(target_date, time.min, tzinfo=zone).astimezone(timezone.utc)
            rows = [{"forecast_instant": start + timedelta(hours=hour)}
                    for hour in range(expected_hours)]
            params = {"start_date": target_date.isoformat(), "end_date": target_date.isoformat()}
            main.validate_hourly_row_coverage(rows, params)
            with self.assertRaisesRegex(ValueError, "Incomplete forecast coverage"):
                main.validate_hourly_row_coverage(rows[:-1], params)


if __name__ == "__main__":
    unittest.main()
