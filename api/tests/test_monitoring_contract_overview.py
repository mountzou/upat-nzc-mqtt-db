import unittest
from datetime import datetime
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
            "event_time": "2026-03-13T21:20:51.941486",
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
            "event_time": "2026-03-14T16:00:00",
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
            "event_time": "2026-03-14T15:00:00",
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

    def test_history_route_exposes_supported_aggregation_parameters(self):
        schema = app.openapi()
        paths = schema["paths"]
        history_parameters = {
            parameter["name"]
            for parameter in paths[
                "/indoor_environment/devices/{device_id}/history"
            ]["get"]["parameters"]
        }

        self.assertTrue(
            {
                "rolling_1h",
                "rolling_24h_hourly",
            }.issubset(history_parameters)
        )

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
                    "event_time": "2026-03-13T21:20:51.941486",
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
                    "event_time": "2026-03-13T21:20:51.941486",
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

    @patch("monitoring.services.service_overview.fetch_device_history")
    def test_device_history_returns_validated_buckets(self, mock_fetch):
        mock_fetch.return_value = AGGREGATED_HISTORY_PAYLOAD

        response = self.client.get(
            "/indoor_environment/devices/portable-112/history?aggregate=avg&bucket_unit=hour&bucket_size=1&limit=24"
        )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["device_id"], AGGREGATED_HISTORY_PAYLOAD["device_id"])
        self.assertEqual(data["count"], AGGREGATED_HISTORY_PAYLOAD["count"])
        self.assertEqual(
            [item["event_time"] for item in data["items"]],
            [
                "2026-03-14T18:00:00+02:00",
                "2026-03-14T17:00:00+02:00",
            ],
        )

    @patch("monitoring.services.service_overview.get_history_window")
    @patch("monitoring.services.service_overview.fetch_device_history")
    def test_device_history_rolling_1h_averages_five_minute_buckets(
        self,
        mock_fetch,
        mock_get_history_window,
    ):
        mock_get_history_window.return_value = (
            datetime(2026, 6, 8, 11, 10, 0),
            datetime(2026, 6, 8, 12, 10, 0),
        )
        mock_fetch.return_value = {
            "device_id": "portable-108",
            "count": 2,
            "items": [
                {
                    "device_id": "portable-108",
                    "event_time": "2026-06-08T12:05:00",
                    "measurements": {
                        "temperature": {"value": 26.0, "unit": "C"},
                        "relative_humidity": {"value": 65.0, "unit": "%"},
                        "co2": {"value": 500.0, "unit": "ppm"},
                        "voc": {"value": 100.0, "unit": None},
                        "pm25": {"value": 4.0, "unit": "ug/m3"},
                    },
                },
                {
                    "device_id": "portable-108",
                    "event_time": "2026-06-08T12:00:00",
                    "measurements": {
                        "temperature": {"value": 28.0, "unit": "C"},
                        "relative_humidity": {"value": 67.0, "unit": "%"},
                        "co2": {"value": 520.0, "unit": "ppm"},
                        "voc": {"value": 120.0, "unit": None},
                        "pm25": {"value": 6.0, "unit": "ug/m3"},
                    },
                },
            ],
        }

        response = self.client.get(
            "/indoor_environment/devices/portable-108/history?rolling_1h=true"
        )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["count"], 1)
        self.assertEqual(data["items"][0]["event_time"], "2026-06-08T15:10:00+03:00")
        measurements = data["items"][0]["measurements"]
        self.assertEqual(measurements["temperature"]["value"], 27.0)
        self.assertEqual(measurements["relative_humidity"]["value"], 66.0)
        self.assertEqual(measurements["co2"]["value"], 510.0)
        self.assertEqual(measurements["voc"]["value"], 110.0)
        self.assertEqual(measurements["pm25"]["value"], 5.0)
        mock_fetch.assert_called_once()
        self.assertEqual(
            mock_fetch.call_args.kwargs["metrics"],
            ("temperature", "relative_humidity", "co2", "voc", "pm25"),
        )
        self.assertEqual(mock_fetch.call_args.kwargs["bucket_size"], 5)
        self.assertEqual(mock_fetch.call_args.kwargs["limit"], 12)
        mock_get_history_window.assert_called_once_with(
            bucket_unit="minute",
            bucket_size=5,
            limit=12,
        )

    @patch("monitoring.services.service_overview.get_history_window")
    @patch("monitoring.services.service_overview.fetch_device_history")
    def test_device_history_rolling_24h_hourly_series(
        self,
        mock_fetch,
        mock_get_history_window,
    ):
        mock_get_history_window.return_value = (
            datetime(2026, 6, 8, 12, 0, 0),
            datetime(2026, 6, 9, 12, 0, 0),
        )
        mock_fetch.return_value = {
            "device_id": "portable-108",
            "count": 4,
            "items": [
                {
                    "device_id": "portable-108",
                    "event_time": "2026-06-09T11:30:00",
                    "measurements": {"temperature": {"value": 24.0, "unit": "C"}},
                },
                {
                    "device_id": "portable-108",
                    "event_time": "2026-06-09T11:45:00",
                    "measurements": {"temperature": {"value": 26.0, "unit": "C"}},
                },
                {
                    "device_id": "portable-108",
                    "event_time": "2026-06-09T10:15:00",
                    "measurements": {"temperature": {"value": 20.0, "unit": "C"}},
                },
                {
                    "device_id": "portable-108",
                    "event_time": "2026-06-08T12:30:00",
                    "measurements": {"temperature": {"value": 22.0, "unit": "C"}},
                },
            ],
        }

        response = self.client.get(
            "/indoor_environment/devices/portable-108/history?rolling_24h_hourly=true"
        )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["count"], 3)
        self.assertEqual(
            [item["event_time"] for item in data["items"]],
            [
                "2026-06-08T15:00:00+03:00",
                "2026-06-09T13:00:00+03:00",
                "2026-06-09T14:00:00+03:00",
            ],
        )
        self.assertEqual(data["items"][0]["measurements"]["temperature"]["value"], 22.0)
        self.assertEqual(data["items"][1]["measurements"]["temperature"]["value"], 20.0)
        self.assertEqual(data["items"][2]["measurements"]["temperature"]["value"], 25.0)
        self.assertEqual(mock_fetch.call_args.kwargs["bucket_size"], 15)
        self.assertEqual(mock_fetch.call_args.kwargs["limit"], 24 * 4)
        mock_get_history_window.assert_called_once_with(
            bucket_unit="minute",
            bucket_size=15,
            limit=24 * 4,
        )

    @patch("monitoring.services.service_overview.fetch_device_history")
    def test_device_history_passes_explicit_custom_window(self, mock_fetch):
        mock_fetch.return_value = {
            "device_id": "portable-112",
            "count": 1,
            "items": [
                {
                    "device_id": "portable-112",
                    "event_time": "2026-07-24T00:00:00",
                    "measurements": {
                        "temperature": {"value": 24.0, "unit": "C"},
                    },
                }
            ],
        }

        response = self.client.get(
            "/indoor_environment/devices/portable-112/history"
            "?aggregate=avg&bucket_unit=day&bucket_size=1&limit=8"
            "&start=2026-07-24T00:00:00&end=2026-08-01T00:00:00"
        )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(
            mock_fetch.call_args.kwargs["start"],
            datetime(2026, 7, 24, 0, 0),
        )
        self.assertEqual(
            mock_fetch.call_args.kwargs["end"],
            datetime(2026, 8, 1, 0, 0),
        )

    @patch("monitoring.routes.indoor_environment.get_device_history")
    def test_device_history_passes_metric_filter(self, mock_history):
        mock_history.return_value = {
            "device_id": "portable-108",
            "count": 1,
            "items": [
                {
                    "device_id": "portable-108",
                    "event_time": "2026-08-02T12:45:00Z",
                    "measurements": {
                        "temperature": {"value": 29.5, "unit": "C"},
                    },
                }
            ],
        }

        response = self.client.get(
            "/indoor_environment/devices/portable-108/history"
            "?aggregate=avg&bucket_unit=minute&bucket_size=1&limit=15"
            "&metric=temperature"
        )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(mock_history.call_args.kwargs["metrics"], ("temperature",))

    @patch("monitoring.services.service_overview.fetch_device_history")
    def test_device_history_invalid_payload_returns_502(self, mock_fetch):
        mock_fetch.return_value = [{"device_id": "portable-112"}]

        response = self.client.get(
            "/indoor_environment/devices/portable-112/history?aggregate=avg&bucket_unit=hour&bucket_size=1&limit=24"
        )

        self.assertEqual(response.status_code, 502)
        self.assertEqual(
            response.json()["detail"],
            "Malformed upstream device payload",
        )

    @patch("monitoring.services.service_overview.fetch_device_history")
    def test_device_history_rejects_mismatched_root_device_id(self, mock_fetch):
        mock_fetch.return_value = {
            **AGGREGATED_HISTORY_PAYLOAD,
            "device_id": "portable-999",
        }

        response = self.client.get(
            "/indoor_environment/devices/portable-112/history?aggregate=avg&bucket_unit=hour&bucket_size=1&limit=24"
        )

        self.assertEqual(response.status_code, 502)
        self.assertEqual(
            response.json()["detail"],
            "Malformed upstream device payload",
        )

    @patch("monitoring.services.service_overview.fetch_device_history")
    def test_device_history_rejects_mixed_device_ids(self, mock_fetch):
        mock_fetch.return_value = {
            **AGGREGATED_HISTORY_PAYLOAD,
            "items": [
                AGGREGATED_HISTORY_PAYLOAD["items"][0],
                {
                    **AGGREGATED_HISTORY_PAYLOAD["items"][1],
                    "device_id": "portable-999",
                },
            ],
        }

        response = self.client.get(
            "/indoor_environment/devices/portable-112/history?aggregate=avg&bucket_unit=hour&bucket_size=1&limit=24"
        )

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
                    "event_time": "2026-03-13T21:20:51.941486",
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

    @patch("monitoring.services.service_overview.fetch_device_history")
    def test_history_uses_winter_athens_offset(self, mock_fetch):
        mock_fetch.return_value = {
            "device_id": "portable-112",
            "count": 2,
            "items": [
                {
                    "device_id": "portable-112",
                    "event_time": "2026-03-14T16:00:00",
                    "measurements": {"temperature": {"value": 16, "unit": "C"}},
                },
                {
                    "device_id": "portable-112",
                    "event_time": "2026-03-14T15:00:00",
                    "measurements": {"temperature": {"value": 15, "unit": "C"}},
                },
            ],
        }
        response = self.client.get(
            "/indoor_environment/devices/portable-112/history?aggregate=avg&bucket_unit=hour&bucket_size=1&limit=24"
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(len(data["items"]), 2)
        self.assertEqual(data["items"][0]["event_time"], "2026-03-14T18:00:00+02:00")
        self.assertEqual(data["items"][1]["event_time"], "2026-03-14T17:00:00+02:00")

    @patch("monitoring.services.service_overview.fetch_device_latest")
    def test_latest_uses_summer_athens_offset(self, mock_fetch):
        mock_fetch.return_value = {
            "device_id": "portable-112",
            "count": 1,
            "items": [
                {
                    "device_id": "portable-112",
                    "event_time": "2026-08-01T19:44:00",
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
