import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from api_control import ApiControl, ApiControlError, DAY, HISTORY, initialize
from main import main, parse_args, run
from fusionsolar import FusionSolarRateLimitError
from test_fusionsolar import FakeResponse, FakeSession


PAYLOAD = {"devTypeId": 1, "devIds": "private1,private2", "startTime": 1000, "endTime": 2000}


class ApiControlTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = self.directory.name
        initialize(self.root, "account", now=0)
        self.now = DAY + 1.0
        self.events = []
        self.delays = []

    def sleep(self, seconds):
        self.delays.append(seconds)
        self.now += seconds

    def control(self, trigger="manual", account="account"):
        return ApiControl(self.root, account, trigger, clock=lambda: self.now,
                          sleep=self.sleep, event_sink=self.events.append)

    def history(self, control, outcome="success"):
        record = control.begin_attempt(HISTORY, PAYLOAD)
        control.finish_attempt(record, {"outcome": outcome})
        return record

    def test_initial_hold_and_no_reset_or_different_account(self):
        self.now = DAY - 1
        with self.control() as c, self.assertRaises(ApiControlError):
            c.preflight(2)
        with self.assertRaises(FileExistsError):
            initialize(self.root, "account", now=self.now)
        with self.assertRaises(ApiControlError):
            with self.control(account="another"):
                pass
        self.now = DAY
        with self.control() as c:
            c.preflight(2)

    def test_missing_and_corrupt_state_fail_without_recreating_database(self):
        path = Path(self.root) / "api-control.sqlite3"
        path.unlink()
        with self.assertRaises(ApiControlError):
            with self.control():
                pass
        self.assertFalse(path.exists())
        path.write_bytes(b"not a sqlite database")
        with self.assertRaises(ApiControlError):
            with self.control():
                pass
        self.assertEqual(b"not a sqlite database", path.read_bytes())

    def test_other_process_cannot_enter_while_any_run_holds_lock(self):
        script = """
import sys
from api_control import ApiControl, ApiControlError
try:
    with ApiControl(sys.argv[1], 'account', 'scheduled'):
        sys.exit(9)
except ApiControlError:
    sys.exit(0)
"""
        with self.control():
            result = subprocess.run([sys.executable, "-c", script, self.root],
                                    capture_output=True, timeout=10)
        self.assertEqual(0, result.returncode, result.stderr)
        with self.control("scheduled") as c:
            c.preflight(2)

    def test_shared_rolling_budget_and_two_calls_reserved_for_scheduled(self):
        first_time = self.now
        for _ in range(5):
            with self.control("backfill") as c:
                c.preflight(2)
                self.history(c)
                self.history(c)
        with self.control("manual") as c, self.assertRaises(ApiControlError):
            c.preflight(1)
        with self.control("scheduled") as c:
            c.preflight(2)
            self.history(c)
            self.history(c)
            self.assertEqual(12, c._count(HISTORY, DAY))
        with self.control("scheduled") as c, self.assertRaises(ApiControlError):
            c.preflight(1)
        # Crossing midnight is irrelevant; only timestamps older than 24h expire.
        self.now = first_time + DAY - 1
        with self.control("scheduled") as c, self.assertRaises(ApiControlError):
            c.preflight(1)
        self.now += 1
        with self.control("scheduled") as c:
            c.preflight(1)
            with self.assertRaises(ApiControlError):
                c.preflight(2)

    def test_full_run_preflight_rejects_insufficient_remaining_budget(self):
        with self.control() as c:
            for _ in range(9):
                self.history(c)
            with self.assertRaises(ApiControlError):
                c.preflight(2)
            self.assertEqual(0, c._count("login", DAY))
            c.preflight(1)

    def test_spacing_survives_process_restart_and_device_type_change(self):
        with self.control() as c:
            self.history(c)
        self.now += 20
        with self.control("scheduled") as c:
            record = c.begin_attempt(HISTORY, {**PAYLOAD, "devTypeId": 17})
            c.finish_attempt(record, {"outcome": "success"})
        self.assertEqual([45], self.delays)

    def test_account_cooldown_is_shared_and_checks_do_not_extend_it(self):
        with self.control() as c:
            self.history(c, "account_rate_limit")
        failure_time = self.now
        for trigger in ("manual", "scheduled", "backfill"):
            self.now += 10
            with self.control(trigger) as c:
                with self.assertRaises(ApiControlError):
                    c.preflight(2)
                until = c.conn.execute("SELECT blocked_until FROM metadata").fetchone()[0]
                self.assertEqual(failure_time + DAY, until)
        self.now = failure_time + DAY
        with self.control() as c:
            c.preflight(2)

    def test_attempt_commit_precedes_network_and_interruption_is_conservative(self):
        with self.control() as c:
            record = c.begin_attempt(HISTORY, PAYLOAD)
            with sqlite3.connect(Path(self.root) / "api-control.sqlite3") as reader:
                row = reader.execute("SELECT finished_at,record_json FROM attempts").fetchone()
            self.assertIsNone(row[0])
            self.assertEqual(record["attempt_id"], json.loads(row[1])["attempt_id"])
        self.now += 100
        recovery_time = self.now
        with self.control() as c:
            with self.assertRaises(ApiControlError):
                c.preflight(2)
            stored = json.loads(c.conn.execute("SELECT record_json FROM attempts").fetchone()[0])
            self.assertEqual("unknown_after_interruption", stored["outcome"])
            self.assertEqual(1, c._count(HISTORY, DAY))
        self.now += 100
        with self.control() as c:
            until = c.conn.execute("SELECT blocked_until FROM metadata").fetchone()[0]
            self.assertEqual(recovery_time + DAY, until)

    def test_login_window_and_metadata_limits_are_also_durable(self):
        with self.control() as c:
            for _ in range(5):
                c.finish_attempt(c.begin_attempt("login", {}), {"outcome": "success"})
            with self.assertRaises(ApiControlError):
                c.begin_attempt("login", {})
            with self.assertRaises(ApiControlError):
                c.preflight(2)
            # The fifth permitted login must still be allowed to fetch history.
            self.history(c)
        self.now += 600
        with self.control() as c:
            c.preflight(2)
            for _ in range(12):
                c.finish_attempt(c.begin_attempt("getDevList", {}), {"outcome": "success"})
        with self.control() as c, self.assertRaises(ApiControlError):
            c.preflight(2)

    def test_failed_attempt_consumes_history_budget(self):
        with self.control() as c:
            for _ in range(9):
                self.history(c)
            self.history(c, "transport_error")
        self.now += 901
        with self.control() as c, self.assertRaises(ApiControlError):
            c.preflight(1)

    def test_live_preview_without_shared_state_never_constructs_http_client(self):
        env = {"FUSIONSOLAR_USERNAME": "account", "FUSIONSOLAR_PLANT_CODE": "NE=private"}
        with patch.dict(os.environ, env, clear=True), patch("main.FusionSolarClient") as client:
            with self.assertRaises(ApiControlError):
                run(parse_args(["--live", "--no-save-to-db"]))
            client.assert_not_called()

    def test_fixture_needs_no_ledger_and_never_constructs_http_client(self):
        fixture = Path(__file__).parent / "tests/fixtures/fusionsolar_sample.json"
        with patch.dict(os.environ, {}, clear=True), patch("main.FusionSolarClient") as client:
            batch = run(parse_args(["--fixture", str(fixture), "--lookback-days", "1"]))
            client.assert_not_called()
            self.assertEqual("fixture", batch["source_kind"])

    def test_live_pipeline_uses_four_accounted_calls_and_meter_pacing(self):
        fixture = json.loads((Path(__file__).parent / "tests/fixtures/fusionsolar_sample.json").read_text())
        session = FakeSession([
            FakeResponse({"success": True}, headers={"XSRF-TOKEN": "private"}),
            FakeResponse(fixture["device_list"]),
            FakeResponse(fixture["history_by_device_type"]["1"]),
            FakeResponse(fixture["history_by_device_type"]["17"]),
        ])
        env = {"FUSIONSOLAR_USERNAME": "account", "FUSIONSOLAR_PLANT_CODE": fixture["plant_code"],
               "FUSIONSOLAR_SYSTEM_CODE": "private", "FUSIONSOLAR_BASE_URL": "https://example.test/thirdData"}
        with patch.dict(os.environ, env, clear=True), \
             patch("main.ApiControl", side_effect=lambda *args: self.control()), \
             patch("fusionsolar.requests.Session", return_value=session):
            batch = run(parse_args(["--live", "--target-date", fixture["target_date"], "--lookback-days", "1"]))
        self.assertEqual("fusion_live", batch["source_kind"])
        self.assertEqual(4, len(session.calls))
        self.assertEqual([65], self.delays)
        with self.control() as c:
            outcomes = [json.loads(row[0])["outcome"] for row in c.conn.execute("SELECT record_json FROM attempts")]
            self.assertEqual(["success"] * 4, outcomes)

    def test_failed_live_pipeline_keeps_attempt_evidence_without_postgres_write(self):
        session = FakeSession([FakeResponse({"success": False, "failCode": 407})])
        env = {"FUSIONSOLAR_USERNAME": "account", "FUSIONSOLAR_PLANT_CODE": "NE=private",
               "FUSIONSOLAR_SYSTEM_CODE": "private", "FUSIONSOLAR_BASE_URL": "https://example.test/thirdData"}
        with patch.dict(os.environ, env, clear=True), \
             patch("main.ApiControl", side_effect=lambda *args: self.control()), \
             patch("fusionsolar.requests.Session", return_value=session), \
             patch("main.persist_batch") as persist:
            with self.assertRaises(FusionSolarRateLimitError):
                main(["--live", "--save-to-db", "--site-key", "upat-pv"])
            persist.assert_not_called()
            # A second run is refused by preflight before another login attempt.
            with self.assertRaises(ApiControlError):
                main(["--live", "--save-to-db", "--site-key", "upat-pv"])
            self.assertEqual(1, len(session.calls))
        with self.control() as c:
            record = json.loads(c.conn.execute("SELECT record_json FROM attempts").fetchone()[0])
            self.assertEqual("account_rate_limit", record["outcome"])


if __name__ == "__main__":
    unittest.main()
