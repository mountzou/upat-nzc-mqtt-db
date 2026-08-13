import io
import json
import unittest
import urllib.error
from contextlib import redirect_stderr
from unittest.mock import Mock, patch

import fetch_open_meteo_forecast as forecast


class Response:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


def http_error(code):
    return urllib.error.HTTPError(
        url="https://api.open-meteo.com/v1/forecast",
        code=code,
        msg="temporary failure",
        hdrs=None,
        fp=None,
    )


class FetchForecastJsonTests(unittest.TestCase):
    @patch("fetch_open_meteo_forecast.urllib.request.urlopen")
    def test_retries_503_with_exponential_backoff_then_succeeds(self, urlopen):
        urlopen.side_effect = [http_error(503), http_error(503), Response({"ok": True})]
        sleep = Mock()

        with redirect_stderr(io.StringIO()) as stderr:
            result = forecast.fetch_forecast_json(
                "https://example.test",
                max_attempts=4,
                backoff_base_s=2,
                backoff_max_s=10,
                sleep_fn=sleep,
            )

        self.assertEqual(result, {"ok": True})
        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [2, 4])
        self.assertIn("HTTP 503", stderr.getvalue())

    @patch("fetch_open_meteo_forecast.urllib.request.urlopen")
    def test_does_not_retry_non_transient_http_error(self, urlopen):
        urlopen.side_effect = http_error(400)
        sleep = Mock()

        with self.assertRaises(urllib.error.HTTPError):
            forecast.fetch_forecast_json(
                "https://example.test",
                max_attempts=4,
                sleep_fn=sleep,
            )

        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    @patch("fetch_open_meteo_forecast.urllib.request.urlopen")
    def test_stops_after_the_configured_attempt_limit(self, urlopen):
        urlopen.side_effect = http_error(503)
        sleep = Mock()

        with redirect_stderr(io.StringIO()):
            with self.assertRaises(urllib.error.HTTPError):
                forecast.fetch_forecast_json(
                    "https://example.test",
                    max_attempts=3,
                    backoff_base_s=1,
                    backoff_max_s=10,
                    sleep_fn=sleep,
                )

        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [1, 2])


if __name__ == "__main__":
    unittest.main()
