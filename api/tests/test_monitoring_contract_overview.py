import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from main import app
from monitoring.routes.auth import get_current_user
from monitoring.services.authentication import AuthUserRecord


# DeviceHistoryResponse-shaped payload for /latest endpoint (one bucket with all metrics).
LATEST_PAYLOAD = {
    "device_id": "portable-112",
    "count": 1,
    "items": [
        {
            "device_id": "portable-112",
            "event_time": "2026-03-13T21:20:51.941486+00:00",
            "measurements": {
                "co2": {"value": 458, "unit": "ppm"},
                "relative_humidity": {"value": 67.83, "unit": "%"},
                "temperature": {"value": 16.18, "unit": "C"},
            },
        },
    ],
}

AGGREGATED_HISTORY_PAYLOAD = {
    "device_id": "portable-112",
    "count": 2,
    "items": [
        {
            "device_id": "portable-112",
            "event_time": "2026-03-14T16:00:00+00:00",
            "measurements": {
                "co2": {"value": 441.8, "unit": "ppm"},
                "pm25": {"value": 0, "unit": "ug/m3"},
                "relative_humidity": {"value": 61.3, "unit": "%"},
                "temperature": {"value": 16, "unit": "C"},
                "voc": {"value": 111.8, "unit": None},
            },
        },
        {
            "device_id": "portable-112",
            "event_time": "2026-03-14T15:00:00+00:00",
            "measurements": {
                "co2": {"value": 430.5, "unit": "ppm"},
                "pm25": {"value": 0, "unit": "ug/m3"},
                "relative_humidity": {"value": 61, "unit": "%"},
                "temperature": {"value": 16, "unit": "C"},
                "voc": {"value": 101.1, "unit": None},
            },
        },
    ],
}


class OverviewRouteTests(unittest.TestCase):
    def setUp(self):
        app.dependency_overrides[get_current_user] = lambda: AuthUserRecord(
            username="test-admin",
            role="system_admin",
        )
        self.client = TestClient(app)

    def tearDown(self):
        app.dependency_overrides.pop(get_current_user, None)
        self.client.close()


    @patch("monitoring.services.service_overview.fetch_device_latest")
    def test_latest_overview_happy_path(self, mock_fetch):
        mock_fetch.return_value = LATEST_PAYLOAD

        response = self.client.get("/indoor_environment/devices/portable-112/latest")

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(
            response.json(),
            {
                "device_id": "portable-112",
                "latest_event_time": "2026-03-13T23:20:51.941486+02:00",
                "readings": {
                    "co2": {"value": 458, "unit": "ppm"},
                    "relative_humidity": {"value": 67.83, "unit": "%"},
                    "temperature": {"value": 16.18, "unit": "C"},
                },
            },
        )

    @patch("monitoring.services.service_overview.fetch_device_latest")
    def test_latest_overview_allows_missing_metrics(self, mock_fetch):
        mock_fetch.return_value = {
            "device_id": "portable-112",
            "count": 1,
            "items": [
                {
                    "device_id": "portable-112",
                    "event_time": "2026-03-13T21:20:51.941486+00:00",
                    "measurements": {"co2": {"value": 458, "unit": "ppm"}},
                },
            ],
        }

        response = self.client.get("/indoor_environment/devices/portable-112/latest")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["readings"],
            {"co2": {"value": 458, "unit": "ppm"}},
        )

    @patch("monitoring.services.service_overview.fetch_device_latest")
    def test_latest_overview_empty_payload_returns_404(self, mock_fetch):
        mock_fetch.return_value = {"device_id": "portable-112", "count": 0, "items": []}

        response = self.client.get("/indoor_environment/devices/portable-112/latest")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(
            response.json()["detail"],
            "No latest data found for device 'portable-112'",
        )

    @patch("monitoring.services.service_overview.fetch_device_latest")
    def test_latest_overview_invalid_row_returns_502(self, mock_fetch):
        mock_fetch.return_value = [{"device_id": "portable-112", "metric": "co2"}]

        response = self.client.get("/indoor_environment/devices/portable-112/latest")

        self.assertEqual(response.status_code, 502)
        self.assertEqual(
            response.json()["detail"],
            "Malformed upstream device payload",
        )

    @patch("monitoring.services.service_overview.fetch_device_latest")
    def test_latest_overview_rejects_mixed_device_ids(self, mock_fetch):
        mock_fetch.return_value = {
            "device_id": "portable-112",
            "count": 2,
            "items": [
                LATEST_PAYLOAD["items"][0],
                {
                    "device_id": "portable-999",
                    "event_time": "2026-03-13T21:20:51.941486+00:00",
                    "measurements": {"temperature": {"value": 16.18, "unit": "C"}},
                },
            ],
        }

        response = self.client.get("/indoor_environment/devices/portable-112/latest")

        self.assertEqual(response.status_code, 502)
        self.assertEqual(
            response.json()["detail"],
            "Malformed upstream device payload",
        )


    @patch("monitoring.services.service_overview.fetch_device_latest")
    def test_latest_uses_winter_athens_offset(self, mock_fetch):
        mock_fetch.return_value = {
            "device_id": "portable-112",
            "count": 1,
            "items": [
                {
                    "device_id": "portable-112",
                    "event_time": "2026-03-13T21:20:51.941486+00:00",
                    "measurements": {
                        "temperature": {"value": 16.18, "unit": "C"},
                    },
                },
            ],
        }
        response = self.client.get("/indoor_environment/devices/portable-112/latest")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["latest_event_time"],
            "2026-03-13T23:20:51.941486+02:00",
        )


    @patch("monitoring.services.service_overview.fetch_device_latest")
    def test_latest_uses_summer_athens_offset(self, mock_fetch):
        mock_fetch.return_value = {
            "device_id": "portable-112",
            "count": 1,
            "items": [
                {
                    "device_id": "portable-112",
                    "event_time": "2026-08-01T19:44:00+00:00",
                    "measurements": {
                        "temperature": {"value": 28.0, "unit": "C"},
                    },
                },
            ],
        }

        response = self.client.get("/indoor_environment/devices/portable-112/latest")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["latest_event_time"],
            "2026-08-01T22:44:00+03:00",
        )


class InternalHistoryValidationTests(unittest.TestCase):
    @patch("monitoring.services.service_overview.fetch_device_history")
    def test_valid_history_keeps_athens_offset(self, fetch):
        from monitoring.services.service_overview import get_device_history
        fetch.return_value = AGGREGATED_HISTORY_PAYLOAD
        result = get_device_history('portable-112', aggregate='avg', interval='1h', limit=24)
        self.assertEqual(result.items[0].event_time.isoformat(), '2026-03-14T18:00:00+02:00')

    @patch("monitoring.services.service_overview.fetch_device_history")
    def test_invalid_or_mixed_identity_is_rejected(self, fetch):
        from fastapi import HTTPException
        from monitoring.services.service_overview import get_device_history
        for payload in (
            [{"device_id": "portable-112"}],
            {**AGGREGATED_HISTORY_PAYLOAD, "device_id": "portable-999"},
            {**AGGREGATED_HISTORY_PAYLOAD, "items": [
                AGGREGATED_HISTORY_PAYLOAD['items'][0],
                {**AGGREGATED_HISTORY_PAYLOAD['items'][1], 'device_id': 'portable-999'}]},
        ):
            with self.subTest(payload=payload):
                fetch.return_value = payload
                with self.assertRaises(HTTPException) as error:
                    get_device_history('portable-112', aggregate='avg', interval='1h', limit=24)
                self.assertEqual(error.exception.status_code, 502)
