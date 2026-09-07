import unittest
import tempfile
import json
from email.utils import formatdate
import requests
from api_control import ApiControl, ApiControlError, initialize, DAY

from fusionsolar import (
    FusionSolarClient,
    FusionSolarError,
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
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append({"url": url, **kwargs})
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def client_with_responses(testcase, responses):
    directory = tempfile.TemporaryDirectory()
    testcase.addCleanup(directory.cleanup)
    initialize(directory.name, "user", now=0)
    current = [DAY + 1.0]
    events = []
    control = ApiControl(directory.name, "user", "manual", clock=lambda: current[0],
                         sleep=lambda seconds: current.__setitem__(0, current[0]+seconds),
                         event_sink=events.append)
    control.__enter__()
    testcase.addCleanup(control.__exit__)
    session = FakeSession(responses)
    client = FusionSolarClient(
        base_url="https://example.test/thirdData",
        username="user",
        system_code="secret",
        session=session,
        control=control,
    )
    return client, session


class FusionSolarClientTests(unittest.TestCase):
    def test_rejects_non_https_or_non_third_data_base_url(self):
        with self.assertRaises(ValueError):
            FusionSolarClient(
                base_url="http://example.test/api",
                username="user",
                system_code="secret",
                control=None,
            )

    def test_login_token_is_reused_and_never_appears_in_call_report(self):
        client, session = client_with_responses(self,
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
        client, session = client_with_responses(self,
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
        client, session = client_with_responses(self,
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

    def test_error_outcomes_are_durable_distinct_and_sanitized(self):
        cases = [
            (FakeResponse({"success": False, "failCode": 407}), "account_rate_limit", DAY),
            (FakeResponse(ValueError("SECRET-RAW"), status=429), "system_rate_limit", 900),
            (FakeResponse({"success": False, "failCode": 429}), "system_rate_limit", 900),
            (FakeResponse(ValueError("SECRET-RAW")), "invalid_json", 900),
            (requests.Timeout("SECRET-URL-TOKEN"), "transport_error", 900),
            (FakeResponse({}, status=302, headers={"Location": "SECRET-URL"}), "redirect_refused", 900),
            (FakeResponse({"success": True, "failCode": 0}), "missing_token", 900),
        ]
        for response, outcome, cooldown in cases:
            with self.subTest(outcome=outcome):
                client, session = client_with_responses(self, [response])
                with self.assertRaises(FusionSolarError):
                    client.login()
                self.assertEqual(1, len(session.calls))
                self.assertFalse(session.calls[0]["allow_redirects"])
                report = client.call_reports[-1]
                self.assertEqual(outcome, report["outcome"])
                row = client.control.conn.execute("SELECT record_json FROM attempts").fetchone()[0]
                self.assertNotIn("SECRET", row)
                self.assertNotIn("secret", row)
                self.assertNotIn("userName", row)
                self.assertEqual(outcome, json.loads(row)["outcome"])
                until = client.control.conn.execute("SELECT blocked_until FROM metadata").fetchone()[0]
                self.assertEqual(client.control.clock()+cooldown, until)
                with self.assertRaises(ApiControlError):
                    client.login()
                self.assertEqual(1, len(session.calls))

    def test_retry_after_seconds_and_http_date_are_honored(self):
        for header in ["172800", formatdate(DAY+1+172800, usegmt=True)]:
            client, session = client_with_responses(self, [
                FakeResponse({}, status=429, headers={"Retry-After": header})])
            with self.assertRaises(FusionSolarRateLimitError):
                client.login()
            self.assertEqual(172800, client.call_reports[-1]["retry_after_seconds"])
            until = client.control.conn.execute("SELECT blocked_until FROM metadata").fetchone()[0]
            self.assertEqual(client.control.clock()+172800, until)

    def test_history_logs_type_and_window_without_device_ids(self):
        client, session = client_with_responses(self, [FakeResponse({"success": False,"failCode":407})])
        client.token = "SECRET-TOKEN"
        with self.assertRaises(FusionSolarRateLimitError):
            client.get_history(device_ids=["SECRET-DEVICE"],device_type=17,start_ms=1000,end_ms=2000)
        report = client.call_reports[-1]
        self.assertEqual(17, report["dev_type_id"])
        self.assertEqual(1000, report["window_start_ms"])
        self.assertEqual(1, report["device_count"])
        self.assertNotIn("SECRET", json.dumps(report))

    def test_unwritable_ledger_prevents_http(self):
        client, session = client_with_responses(self, [])
        client.control.conn.execute("PRAGMA query_only=ON")
        with self.assertRaisesRegex(ApiControlError, "no request sent"):
            client.login()
        self.assertEqual([], session.calls)

    def test_outcome_write_failure_retains_pending_attempt_for_recovery(self):
        client, session = client_with_responses(self, [
            FakeResponse({"success": True}, headers={"XSRF-TOKEN": "private"})])
        original_post = session.post
        def fail_ledger_after_sending(*args, **kwargs):
            response = original_post(*args, **kwargs)
            client.control.conn.execute("PRAGMA query_only=ON")
            return response
        session.post = fail_ledger_after_sending
        with self.assertRaisesRegex(ApiControlError, "attempt remains pending"):
            client.login()
        self.assertEqual(1, len(session.calls))
        self.assertIsNone(client.control.conn.execute("SELECT finished_at FROM attempts").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
