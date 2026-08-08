import unittest
from datetime import date, datetime, timedelta
from unittest.mock import patch

import main


class WeatherCollectorHorizonTests(unittest.TestCase):
    def test_default_forecast_window_has_one_buffer_day(self):
        self.assertEqual(main.OPEN_METEO_FORECAST_DAYS, 8)

    @patch.object(main, "datetime")
    def test_local_forecast_dates_span_eight_inclusive_dates(self, mocked_datetime):
        mocked_datetime.now.return_value = datetime(2026, 8, 8, 22, 50)

        start_date, end_date = main.local_forecast_dates()

        self.assertEqual(start_date, date(2026, 8, 8))
        self.assertEqual(end_date, date(2026, 8, 15))
        self.assertEqual((end_date - start_date).days + 1, 8)

    @patch.object(main, "local_forecast_dates")
    def test_request_uses_explicit_eight_day_bounds(self, local_forecast_dates):
        local_forecast_dates.return_value = (
            date(2026, 8, 8),
            date(2026, 8, 15),
        )

        params, url = main.build_forecast_request()

        self.assertEqual(params["start_date"], "2026-08-08")
        self.assertEqual(params["end_date"], "2026-08-15")
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


if __name__ == "__main__":
    unittest.main()
