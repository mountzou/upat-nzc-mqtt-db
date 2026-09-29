import io
import sys
import unittest
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

import main as forecast
import joblib
import pandas as pd


class OperationalRandomForestContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.model = joblib.load(forecast.MODEL_PATH)
        cls.features = list(cls.model.feature_names_in_)

    def weather_frame(self):
        return pd.DataFrame({
            "time": ["2026-08-20T00:00", "2026-08-20T12:00"],
            "temperature_2m": [20.0, 30.0],
            "shortwave_radiation": [0.0, 700.0],
            "direct_normal_irradiance": [0.0, 800.0],
            "diffuse_radiation": [0.0, 100.0],
            "cloud_cover": [10.0, 10.0],
            "wind_speed_10m": [2.0, 2.0],
        })

    def test_operational_artifact_is_random_forest_without_xgboost(self):
        self.assertEqual(type(self.model).__name__, "RandomForestRegressor")
        self.assertTrue(type(self.model).__module__.startswith("sklearn."))
        self.assertFalse(any(
            name == "xgboost" or name.startswith("xgboost.") for name in sys.modules
        ))

    def test_persisted_features_match_current_model_without_legacy_lag(self):
        with patch.object(forecast, "get_weather_forecast", return_value=self.weather_frame()):
            result = forecast.run_forecast()
        self.assertEqual(set(result["raw_features"].iloc[1]), set(self.features))
        self.assertNotIn("lag_1h", result.columns)

    def test_fixed_threshold_is_applied(self):
        weather = self.weather_frame()
        weather["shortwave_radiation"] = [19.0, 20.0]
        with (
            patch.object(forecast, "get_weather_forecast", return_value=weather),
            patch.object(forecast, "save_forecast_to_db") as save,
            patch.object(sys, "argv", ["forecast-pv"]),
            redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(forecast.main(), 0)
        result = save.call_args.args[0]
        self.assertEqual(result["predicted_power_kw"].iloc[0], 0.0)
        self.assertGreater(result["predicted_power_kw"].iloc[1], 0.0)

    def test_saves_null_legacy_columns_without_lag_in_json(self):
        with patch.object(forecast, "get_weather_forecast", return_value=self.weather_frame()):
            frame = forecast.run_forecast()
        connection = MagicMock()
        cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (17,)
        with patch.object(forecast, "db_connect", return_value=connection):
            run_id = forecast.save_forecast_to_db(frame)

        self.assertEqual(run_id, 17)
        self.assertEqual(cursor.execute.call_count, 3)
        run_params = cursor.execute.call_args_list[0].args[1]
        self.assertEqual(run_params[1:3], (forecast.DEFAULT_LAT, forecast.DEFAULT_LON))
        self.assertEqual(run_params[9].adapted["latitude"], forecast.DEFAULT_LAT)
        self.assertEqual(run_params[9].adapted["longitude"], forecast.DEFAULT_LON)
        self.assertIsNone(run_params[4])
        self.assertEqual(run_params[7], "model.pkl")
        self.assertIsNone(run_params[8])
        self.assertEqual(run_params[9].adapted["model_version"], "rf_operational_20260806")
        self.assertEqual(run_params[5], 20.0)
        self.assertEqual(run_params[9].adapted["night_ghi_threshold_wm2"], 20.0)
        self.assertNotIn("lag_1h_kw", run_params[9].adapted)
        for index, call in enumerate(cursor.execute.call_args_list[1:]):
            params = call.args[1]
            self.assertIsNone(params[11])
            self.assertEqual(
                params[12].adapted,
                {key: float(frame.iloc[index][key]) for key in self.features},
            )

    def test_preview_computes_and_displays_without_saving(self):
        with (
            patch.object(forecast, "get_weather_forecast", return_value=self.weather_frame()) as weather,
            patch.object(forecast, "save_forecast_to_db") as save,
            patch.object(forecast, "print_forecast") as display,
            patch.object(sys, "argv", ["forecast-pv", "--no-save-to-db"]),
        ):
            self.assertEqual(forecast.main(), 0)
        weather.assert_called_once()
        display.assert_called_once()
        self.assertIn("predicted_power_kw", display.call_args.args[0])
        save.assert_not_called()


if __name__ == "__main__":
    unittest.main()
