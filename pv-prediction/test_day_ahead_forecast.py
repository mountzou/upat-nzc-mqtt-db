import sys
import unittest
from types import ModuleType
from unittest.mock import patch

import day_ahead_forecast as forecast
import joblib
import pandas as pd
from pandas.testing import assert_frame_equal


class OperationalRandomForestContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.model = joblib.load(forecast.MODEL_PATH)
        cls.features = forecast.load_feature_columns(
            cls.model,
            forecast.FEATURES_PATH,
        )

    def test_operational_artifact_is_random_forest_without_xgboost(self):
        self.assertEqual(type(self.model).__name__, "RandomForestRegressor")
        self.assertTrue(type(self.model).__module__.startswith("sklearn."))
        self.assertFalse(
            any(
                name == "xgboost" or name.startswith("xgboost.")
                for name in sys.modules
            )
        )

    def test_legacy_lag_does_not_change_current_rf_prediction(self):
        raw = pd.DataFrame(
            {
                "time": ["2026-08-20T12:00"],
                "temperature_2m": [30.0],
                "shortwave_radiation": [700.0],
                "direct_normal_irradiance": [800.0],
                "diffuse_radiation": [100.0],
                "cloud_cover": [10.0],
                "wind_speed_10m": [2.0],
            }
        )

        lag_zero = forecast.engineer_features(raw, lag_1h_kw=0.0)
        lag_five = forecast.engineer_features(raw, lag_1h_kw=5.0)

        self.assertNotIn("lag_1h", self.features)
        self.assertEqual(lag_zero.loc[0, "lag_1h"], 0.0)
        self.assertEqual(lag_five.loc[0, "lag_1h"], 5.0)
        assert_frame_equal(
            lag_zero[self.features],
            lag_five[self.features],
        )
        self.assertEqual(
            self.model.predict(lag_zero[self.features]).tolist(),
            self.model.predict(lag_five[self.features]).tolist(),
        )

    def test_persisted_raw_features_cover_current_rf_contract(self):
        raw = pd.DataFrame(
            {
                "time": ["2026-08-20T12:00"],
                "temperature_2m": [30.0],
                "shortwave_radiation": [700.0],
                "direct_normal_irradiance": [800.0],
                "diffuse_radiation": [100.0],
                "cloud_cover": [10.0],
                "wind_speed_10m": [2.0],
            }
        )
        engineered = forecast.engineer_features(raw, lag_1h_kw=5.0)
        persisted = forecast.build_raw_features(engineered.iloc[0])

        self.assertFalse(set(self.features) - set(persisted))
        self.assertIn("doy_sin", persisted)
        self.assertIn("doy_cos", persisted)
        self.assertIn("lag_1h", persisted)

    def test_missing_legacy_lag_is_preserved_as_null_metadata(self):
        with patch.dict(
            forecast.os.environ,
            {"PV_LAG_1H_KW": "", "PV_LATEST_ACTIVE_POWER_KW": ""},
        ):
            lag_1h_kw = forecast.resolve_lag_1h_kw(None, None)

        raw = pd.DataFrame(
            {
                "time": ["2026-08-20T00:00"],
                "temperature_2m": [30.0],
                "shortwave_radiation": [700.0],
                "direct_normal_irradiance": [800.0],
                "diffuse_radiation": [100.0],
                "cloud_cover": [10.0],
                "wind_speed_10m": [2.0],
            }
        )
        engineered = forecast.engineer_features(raw, lag_1h_kw=lag_1h_kw)
        persisted = forecast.build_raw_features(engineered.iloc[0])

        self.assertIsNone(lag_1h_kw)
        self.assertTrue(pd.isna(engineered.loc[0, "lag_1h"]))
        self.assertIsNone(persisted["lag_1h"])

    def test_non_finite_legacy_lag_is_normalized_or_rejected(self):
        with patch.dict(
            forecast.os.environ,
            {"PV_LAG_1H_KW": "nan", "PV_LATEST_ACTIVE_POWER_KW": ""},
        ):
            self.assertIsNone(forecast.resolve_lag_1h_kw(None, None))

        with (
            patch.dict(
                forecast.os.environ,
                {"PV_LAG_1H_KW": "inf", "PV_LATEST_ACTIVE_POWER_KW": ""},
            ),
            self.assertRaisesRegex(ValueError, "must be finite or empty"),
        ):
            forecast.resolve_lag_1h_kw(None, None)

    def test_nan_legacy_lag_reaches_sql_and_json_as_null(self):
        raw = pd.DataFrame(
            {
                "time": ["2026-08-20T00:00"],
                "temperature_2m": [30.0],
                "shortwave_radiation": [0.0],
                "direct_normal_irradiance": [0.0],
                "diffuse_radiation": [0.0],
                "cloud_cover": [10.0],
                "wind_speed_10m": [2.0],
            }
        )
        forecast_df = forecast.engineer_features(raw, lag_1h_kw=float("nan"))
        forecast_df["predicted_power_kw"] = 0.0

        class FakeCursor:
            def __init__(self):
                self.calls = []

            def __enter__(self):
                return self

            def __exit__(self, _exc_type, _exc, _tb):
                return False

            def execute(self, query, params):
                self.calls.append((query, params))

            def fetchone(self):
                return (17,)

        class FakeConnection:
            def __init__(self, cursor):
                self._cursor = cursor

            def __enter__(self):
                return self

            def __exit__(self, _exc_type, _exc, _tb):
                return False

            def cursor(self):
                return self._cursor

        def passthrough_json(value):
            return value

        fake_psycopg2 = ModuleType("psycopg2")
        fake_extras = ModuleType("psycopg2.extras")
        fake_extras.Json = passthrough_json
        fake_psycopg2.extras = fake_extras
        cursor = FakeCursor()
        connection = FakeConnection(cursor)

        with (
            patch.dict(
                sys.modules,
                {"psycopg2": fake_psycopg2, "psycopg2.extras": fake_extras},
            ),
            patch.object(forecast, "db_connect", return_value=connection),
        ):
            run_id = forecast.save_forecast_to_db(
                forecast_df,
                latitude=37.068,
                longitude=22.026,
                forecast_days=2,
                lag_1h_kw=float("nan"),
                apply_night_ghi_mask=True,
                night_ghi_threshold_wm2=20.0,
            )

        self.assertEqual(run_id, 17)
        self.assertEqual(len(cursor.calls), 2)
        run_params = cursor.calls[0][1]
        hourly_params = cursor.calls[1][1]
        self.assertIsNone(run_params[4])
        self.assertIsNone(run_params[9]["lag_1h_kw"])
        self.assertIsNone(hourly_params[11])
        self.assertIsNone(hourly_params[12]["lag_1h"])


if __name__ == "__main__":
    unittest.main()
