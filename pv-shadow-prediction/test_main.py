import json
import math
import re
import unittest
from datetime import date, datetime
from pathlib import Path

import main as shadow


class ShadowForecastContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = shadow.load_json(shadow.DEFAULT_REGISTRY_PATH)
        cls.baseline = shadow.load_json(shadow.DEFAULT_FREEZE_PATH)
        cls.spec = shadow.candidate_spec(cls.registry, "champion-replay")
        cls.model, cls.features, cls.hashes = shadow.verify_candidate_artifacts(
            cls.spec,
            cls.baseline,
            shadow.DEFAULT_ARTIFACT_DIR,
        )

    def build_rows(self):
        target = date(2026, 8, 20)
        rows = []
        for hour in range(24):
            daylight = 7 <= hour <= 19
            rows.append(
                {
                    "source_champion_run_id": 100,
                    "forecast_timestamp": datetime(2026, 8, 20, hour),
                    "forecast_date": target,
                    "forecast_hour": hour,
                    "champion_power_kw": 0.0,
                    "raw_features": {
                        "temperature_2m": 28.0,
                        "shortwave_radiation": 600.0 if daylight else 0.0,
                        "direct_normal_irradiance": 700.0 if daylight else 0.0,
                        "diffuse_radiation": 100.0 if daylight else 0.0,
                        "cloud_cover": 10.0,
                        "wind_speed_10m": 2.0,
                        "hour_sin": math.sin(2 * math.pi * hour / 24),
                        "hour_cos": math.cos(2 * math.pi * hour / 24),
                        "doy_sin": math.sin(2 * math.pi * 232 / 365.25),
                        "doy_cos": math.cos(2 * math.pi * 232 / 365.25),
                        "lag_1h": None,
                    },
                }
            )
        return rows

    def test_frozen_replay_artifacts_are_verified(self):
        self.assertEqual(
            self.hashes["model"],
            self.baseline["champion"]["model"]["sha256"],
        )
        self.assertEqual(
            self.hashes["features"],
            self.baseline["champion"]["features"]["sha256"],
        )

    def test_replay_uses_one_complete_champion_snapshot(self):
        rows = self.build_rows()
        run = {
            "id": 100,
            "forecast_date": date(2026, 8, 20),
            "model_artifact": self.baseline["champion"]["model"]["path"],
            "features_artifact": self.baseline["champion"]["features"]["path"],
            "night_ghi_threshold_wm2": 20.0,
            "raw_request": {"apply_night_ghi_mask": True},
        }
        shadow.validate_champion_snapshot(
            run,
            rows,
            self.baseline,
            self.features,
        )
        predictions = shadow.predict_candidate(
            self.model,
            self.features,
            rows,
            self.spec["night_ghi_threshold_wm2"],
        )
        for source, prediction in zip(rows, predictions):
            source["champion_power_kw"] = prediction["predicted_power_kw"]
        shadow.validate_control_replay(predictions, rows)
        self.assertEqual(len(predictions), 24)
        self.assertTrue(all(row["predicted_power_kw"] >= 0 for row in predictions))
        self.assertTrue(
            all(
                predictions[hour]["predicted_power_kw"] == 0
                for hour in list(range(7)) + list(range(20, 24))
            )
        )
        self.assertRegex(shadow.snapshot_sha256(rows), r"^[0-9a-f]{64}$")

        rows[12]["champion_power_kw"] += 0.1
        with self.assertRaisesRegex(ValueError, "does not reproduce"):
            shadow.validate_control_replay(predictions, rows)

    def test_incomplete_champion_snapshot_is_rejected(self):
        rows = self.build_rows()[:-1]
        run = {
            "id": 100,
            "forecast_date": date(2026, 8, 20),
            "model_artifact": self.baseline["champion"]["model"]["path"],
            "features_artifact": self.baseline["champion"]["features"]["path"],
            "night_ghi_threshold_wm2": 20.0,
            "raw_request": {"apply_night_ghi_mask": True},
        }
        with self.assertRaisesRegex(ValueError, "24 rows"):
            shadow.validate_champion_snapshot(
                run,
                rows,
                self.baseline,
                self.features,
            )

    def test_missing_day_of_year_features_are_reconstructed_from_timestamp(self):
        rows = self.build_rows()
        for row in rows:
            row["raw_features"].pop("doy_sin")
            row["raw_features"].pop("doy_cos")
        materialized, reconstructed = shadow.materialize_deterministic_features(rows)
        self.assertEqual(["doy_cos", "doy_sin"], reconstructed)
        self.assertTrue(
            all(
                row["raw_features"]["doy_sin"] is not None
                and row["raw_features"]["doy_cos"] is not None
                for row in materialized
            )
        )

    def test_code_never_mutates_champion_tables(self):
        source = Path(shadow.__file__).read_text(encoding="utf-8")
        mutation_targets = re.findall(
            r"(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+([a-z_]+)",
            source,
            flags=re.IGNORECASE,
        )
        self.assertTrue(mutation_targets)
        self.assertTrue(
            all(target.startswith("pv_shadow_") for target in mutation_targets),
            mutation_targets,
        )


if __name__ == "__main__":
    unittest.main()
