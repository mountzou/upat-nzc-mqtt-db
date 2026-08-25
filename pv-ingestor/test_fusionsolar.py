import unittest

from fusionsolar import (
    FusionSolarClient,
    FusionSolarAuthenticationError,
    FusionSolarRateLimitError,
)


class FakeResponse:
    def __init__(self, body, *, status=200, headers=None, cookies=None):
        self._body = body
        self.status_code = status
        self.headers = headers or {}
        self.cookies = cookies or {}
        self.content = b"{}"

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append({"url": url, **kwargs})
        return self.responses.pop(0)


def client_with_responses(responses):
    session = FakeSession(responses)
    client = FusionSolarClient(
        base_url="https://example.test/thirdData",
        username="user",
        system_code="secret",
        session=session,
    )
    return client, session


class FusionSolarClientTests(unittest.TestCase):
    def test_rejects_non_https_or_non_third_data_base_url(self):
        with self.assertRaises(ValueError):
            FusionSolarClient(
                base_url="http://example.test/api",
                username="user",
                system_code="secret",
            )

    def test_login_token_is_reused_and_never_appears_in_call_report(self):
        client, session = client_with_responses(
            [
                FakeResponse(
                    {"success": True, "failCode": 0},
                    headers={"XSRF-TOKEN": "private-token"},
                ),
                FakeResponse({"success": True, "failCode": 0, "data": []}),
            ]
        )

        client.login()
        client.get_device_list("NE=plant")

        self.assertEqual(2, len(session.calls))
        self.assertNotIn("XSRF-TOKEN", session.calls[0]["headers"])
        self.assertEqual(
            "private-token",
            session.calls[1]["headers"]["XSRF-TOKEN"],
        )
        self.assertNotIn("private-token", repr(client.call_reports))
        with self.assertRaises(FusionSolarAuthenticationError):
            client.login()

    def test_rate_limit_fails_without_retrying(self):
        client, session = client_with_responses(
            [
                FakeResponse(
                    {"success": True, "failCode": 0},
                    headers={"XSRF-TOKEN": "private-token"},
                ),
                FakeResponse({"success": False, "failCode": 407}),
            ]
        )
        client.login()

        with self.assertRaises(FusionSolarRateLimitError):
            client.get_device_list("NE=plant")

        self.assertEqual(2, len(session.calls))
        self.assertEqual(0, client.call_reports[-1]["automatic_retries"])

    def test_one_history_call_batches_same_type_device_ids(self):
        client, session = client_with_responses(
            [
                FakeResponse(
                    {"success": True, "failCode": 0},
                    headers={"XSRF-TOKEN": "private-token"},
                ),
                FakeResponse({"success": True, "failCode": 0, "data": []}),
            ]
        )
        client.login()

        client.get_history(
            device_ids=["101", "102"],
            device_type=1,
            start_ms=1000,
            end_ms=2000,
        )

        payload = session.calls[1]["json"]
        self.assertEqual("101,102", payload["devIds"])
        self.assertEqual(1, payload["devTypeId"])
        self.assertEqual(1000, payload["startTime"])
        self.assertEqual(2000, payload["endTime"])


if __name__ == "__main__":
    unittest.main()
