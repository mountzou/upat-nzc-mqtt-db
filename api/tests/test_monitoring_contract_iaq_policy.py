import unittest

from fastapi.testclient import TestClient

from main import app
from monitoring.policies.iaq import (
    IAQ_POLICY_VERSION,
    build_iaq_policy_payload,
    get_iaq_threshold,
)


class IAQPolicyTests(unittest.TestCase):
    def test_canonical_thresholds_and_period_mapping(self):
        payload = build_iaq_policy_payload()

        self.assertEqual(payload["version"], IAQ_POLICY_VERSION)
        self.assertEqual(get_iaq_threshold("co2", "1h"), 750.0)
        self.assertEqual(get_iaq_threshold("co2", "24h"), 800.0)
        self.assertEqual(get_iaq_threshold("pm25", "1h"), 10.0)
        self.assertEqual(get_iaq_threshold("pm25", "24h"), 15.0)
        self.assertEqual(
            payload["metrics"]["co2"]["thresholds"],
            {"1m": 750.0, "1h": 750.0, "24h": 800.0, "7d": 800.0, "14d": 800.0},
        )
        self.assertEqual(
            payload["metrics"]["pm25"]["thresholds"],
            {"1m": 10.0, "1h": 10.0, "24h": 15.0, "7d": 15.0, "14d": 15.0},
        )

    def test_policy_endpoint_exposes_the_canonical_payload(self):
        with TestClient(app) as client:
            response = client.get("/indoor_environment/policy")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), build_iaq_policy_payload())
        self.assertEqual(response.headers["cache-control"], "public, max-age=3600")
