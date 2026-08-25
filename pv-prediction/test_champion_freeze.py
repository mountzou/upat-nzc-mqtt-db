import csv
import hashlib
import json
import math
import re
import unittest
from collections import Counter
from pathlib import Path

import joblib
import numpy as np


DIR = Path(__file__).resolve().parent
FREEZE_PATH = DIR / "pv_champion_freeze_v1.json"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class ChampionFreezeContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.freeze = json.loads(FREEZE_PATH.read_text(encoding="utf-8"))

    def test_artifact_hashes_and_feature_contract_are_frozen(self):
        self.assertEqual("pv-champion-freeze.v1", self.freeze["schema_version"])
        champion = self.freeze["champion"]
        for key in ("model", "features", "training_manifest"):
            artifact = champion[key]
            path = DIR / artifact["path"]
            self.assertTrue(path.is_file(), path)
            self.assertEqual(artifact["bytes"], path.stat().st_size)
            self.assertEqual(artifact["sha256"], sha256_file(path))

        manifest = json.loads(
            (DIR / champion["training_manifest"]["path"]).read_text(
                encoding="utf-8"
            )
        )
        feature_value = joblib.load(DIR / champion["features"]["path"])
        if isinstance(feature_value, np.ndarray):
            features = [str(value) for value in feature_value.tolist()]
        else:
            features = [str(value) for value in feature_value]
        self.assertEqual(manifest["feature_contract"], features)
        self.assertEqual(self.freeze["inference_contract"]["feature_contract"], features)

        model = joblib.load(DIR / champion["model"]["path"])
        self.assertEqual("RandomForestRegressor", type(model).__name__)
        self.assertTrue(type(model).__module__.startswith("sklearn."))
        self.assertEqual(champion["model_version"], manifest["model_version"])

        provenance = self.freeze["production_provenance"]
        self.assertFalse(provenance["checkout_worktree_clean"])
        self.assertRegex(provenance["checkout_git_head"], r"^[0-9a-f]{40}$")
        self.assertRegex(provenance["image"]["id"], r"^sha256:[0-9a-f]{64}$")
        self.assertFalse(provenance["image_source_fully_attributable_to_checkout_git_head"])

    def test_hourly_snapshot_recomputes_frozen_metrics_and_coverage(self):
        baseline = self.freeze["baseline"]
        snapshot = baseline["hourly_snapshot"]
        csv_path = DIR / snapshot["path"]
        self.assertEqual(snapshot["sha256"], sha256_file(csv_path))

        with csv_path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(snapshot["rows"], len(rows))
        self.assertEqual(240, len(rows))

        keys = {(row["target_date"], int(row["forecast_hour"])) for row in rows}
        self.assertEqual(240, len(keys))
        per_date = Counter(row["target_date"] for row in rows)
        self.assertEqual(
            {target_date: 24 for target_date in baseline["target_dates"]},
            dict(per_date),
        )
        run_map = {
            item["target_date"]: int(item["run_id"])
            for item in baseline["runs"]
        }
        self.assertTrue(
            all(int(row["run_id"]) == run_map[row["target_date"]] for row in rows)
        )

        forecast = [float(row["forecast_power_kw"]) for row in rows]
        actual = [float(row["actual_power_kw"]) for row in rows]
        errors = [predicted - observed for predicted, observed in zip(forecast, actual)]
        actual_energy = sum(actual)
        forecast_energy = sum(forecast)
        recomputed = {
            "actual_energy_kwh": actual_energy,
            "forecast_energy_kwh": forecast_energy,
            "energy_error_kwh": forecast_energy - actual_energy,
            "energy_bias_percent": 100 * sum(errors) / actual_energy,
            "hourly_mae_kw": sum(abs(error) for error in errors) / len(errors),
            "hourly_rmse_kw": math.sqrt(
                sum(error * error for error in errors) / len(errors)
            ),
            "hourly_wape_percent": 100
            * sum(abs(error) for error in errors)
            / actual_energy,
        }
        for name, value in recomputed.items():
            self.assertTrue(
                math.isclose(value, baseline["metrics"][name], abs_tol=1e-10),
                f"{name}: {value} != {baseline['metrics'][name]}",
            )

        eligibility = baseline["eligibility"]
        incomplete = [
            {
                "target_date": row["target_date"],
                "forecast_hour": int(row["forecast_hour"]),
                "observed_5m_count": int(row["observed_5m_count"]),
                "expected_5m_count": 12,
                "saved_ghi_wm2": float(row["saved_ghi_wm2"]),
            }
            for row in rows
            if float(row["saved_ghi_wm2"]) >= 20
            and int(row["observed_5m_count"]) < 12
        ]
        expected_incomplete = eligibility["incomplete_forecast_daylight_hours"]
        self.assertEqual(len(expected_incomplete), len(incomplete))
        for expected, actual_item in zip(expected_incomplete, incomplete):
            self.assertEqual(expected["target_date"], actual_item["target_date"])
            self.assertEqual(expected["forecast_hour"], actual_item["forecast_hour"])
            self.assertEqual(expected["observed_5m_count"], actual_item["observed_5m_count"])
            self.assertEqual(expected["expected_5m_count"], actual_item["expected_5m_count"])
            self.assertTrue(
                math.isclose(
                    float(expected["saved_ghi_wm2"]),
                    actual_item["saved_ghi_wm2"],
                    abs_tol=1e-12,
                )
            )
        self.assertFalse(eligibility["strict_forecast_daylight_complete"])


if __name__ == "__main__":
    unittest.main()
