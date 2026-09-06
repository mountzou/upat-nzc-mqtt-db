"""Tests for thermal comfort API."""

import unittest

from fastapi.testclient import TestClient

from main import app
from monitoring.services.environment.thermal_comfort import (
    calc_discomfort_index,
    calc_hum_index,
)


class TestCalcDiscomfortIndex(unittest.TestCase):
    """Test calc_discomfort_index service function."""

    def test_returns_di_and_condition(self):
        di_val, condition = calc_discomfort_index(tdb=25, rh=50)
        self.assertIsInstance(di_val, float)
        self.assertIsInstance(condition, str)
        self.assertGreater(di_val, 0)
        self.assertTrue(
            "discomfort" in condition.lower() or "no discomfort" in condition.lower()
        )

    def test_low_temperature_low_rh(self):
        di_val, condition = calc_discomfort_index(tdb=20, rh=40)
        self.assertLess(di_val, 25)
        self.assertIsInstance(condition, str)


class TestCalcHumidex(unittest.TestCase):
    """Test calc_hum_index service function."""

    def test_returns_humidex_and_condition(self):
        hum_val, condition = calc_hum_index(tdb=25, rh=50)
        self.assertIsInstance(hum_val, float)
        self.assertIsInstance(condition, str)
        self.assertGreater(hum_val, 0)


class TestThermalComfortEndpoints(unittest.TestCase):
    """Test thermal comfort API endpoints."""

    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def test_discomfort_index_ok(self):
        response = self.client.get(
            "/thermal-comfort/discomfort-index",
            params={"tdb": 25, "rh": 50},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("di", data)
        self.assertIn("discomfort_condition", data)
        self.assertIsInstance(data["di"], (int, float))
        self.assertIsInstance(data["discomfort_condition"], str)

    def test_discomfort_index_missing_params(self):
        response = self.client.get("/thermal-comfort/discomfort-index")
        self.assertEqual(response.status_code, 422)

    def test_discomfort_index_invalid_rh(self):
        response = self.client.get(
            "/thermal-comfort/discomfort-index",
            params={"tdb": 25, "rh": 150},
        )
        self.assertEqual(response.status_code, 422)

    def test_humidex_ok(self):
        response = self.client.get(
            "/thermal-comfort/humidex",
            params={"tdb": 25, "rh": 50},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("humidex", data)
        self.assertIn("discomfort", data)
        self.assertIsInstance(data["humidex"], (int, float))
        self.assertIsInstance(data["discomfort"], str)

    def test_humidex_missing_params(self):
        response = self.client.get("/thermal-comfort/humidex")
        self.assertEqual(response.status_code, 422)
