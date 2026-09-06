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
        self.assertEqual(get_iaq_threshold("co2", "short"), 750.0)
        self.assertEqual(get_iaq_threshold("co2", "long"), 800.0)
        self.assertEqual(get_iaq_threshold("pm25", "short"), 10.0)
        self.assertEqual(get_iaq_threshold("pm25", "long"), 15.0)
        self.assertEqual(
            payload["metrics"]["co2"]["thresholds"]["short"]["period_ids"],
            ["1m", "1h"],
        )
        self.assertEqual(
            payload["metrics"]["co2"]["thresholds"]["long"]["period_ids"],
            ["24h", "7d", "14d"],
        )

    def test_policy_endpoint_exposes_the_canonical_payload(self):
        with TestClient(app) as client:
            response = client.get("/indoor_environment/policy")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), build_iaq_policy_payload())
        self.assertEqual(response.headers["cache-control"], "public, max-age=3600")
