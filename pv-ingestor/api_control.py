"""Durable, account-scoped API accounting. Never connects to PostgreSQL or Huawei."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
import uuid
from datetime import datetime, timezone

DAY = 86400
HISTORY = "getDevHistoryKpi"
POLICY = {
    "history_limit_24h": 12,
    "scheduled_reserve": 2,
    "history_spacing_seconds": 65,
    "login_limit_10m": 5,
    "device_list_limit_24h": 12,
    "account_cooldown_seconds": DAY,
    "system_cooldown_seconds": 900,
    "uncertain_cooldown_seconds": 900,
}


class ApiControlError(RuntimeError):
    """Local refusal: no provider request should be sent."""


def emit(record):
    print(json.dumps(record, sort_keys=True), file=sys.stderr, flush=True)


def account_key(username):
    if not username.strip():
        raise ApiControlError("API account is required")
    return hashlib.sha256(username.strip().lower().encode()).hexdigest()


def initialize(state_dir, username, *, now=None):
    """Explicit one-time operation; unknown previous usage requires a 24h hold."""
    now = time.time() if now is None else now
    root = Path(state_dir)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    key = account_key(username)
    path = root / "api-control.sqlite3"
    # Refuse to replace/reset an existing ledger, including incomplete initialization.
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)
    with sqlite3.connect(path) as conn:
        conn.executescript("""
            CREATE TABLE metadata (id INTEGER PRIMARY KEY CHECK(id=1),
                version INTEGER NOT NULL, account_key TEXT NOT NULL,
                blocked_until REAL NOT NULL, policy TEXT NOT NULL);
            CREATE TABLE attempts (id TEXT PRIMARY KEY, run_id TEXT NOT NULL,
                endpoint TEXT NOT NULL, started_at REAL NOT NULL,
                finished_at REAL, record_json TEXT NOT NULL);
            CREATE INDEX attempts_endpoint_time ON attempts(endpoint, started_at);
        """)
        conn.execute("INSERT INTO metadata VALUES (1,1,?,?,?)",
                     (key, now + DAY, json.dumps(POLICY)))
    (root / "api-control.lock").touch(mode=0o600, exist_ok=False)


class ApiControl:
    def __init__(self, state_dir, username, trigger_kind, *, clock=time.time,
                 sleep=time.sleep, event_sink=emit):
        if not state_dir:
            raise ApiControlError("PV_API_STATE_DIR is required for every live run")
        if trigger_kind not in {"scheduled", "manual", "backfill", "migration"}:
            raise ApiControlError("invalid trigger kind")
        self.root = Path(state_dir)
        self.key = account_key(username)
        self.trigger = trigger_kind
        self.clock, self.sleep, self.emit = clock, sleep, event_sink
        self.run_id = uuid.uuid4().hex
        self.conn = self.lock = None

    def __enter__(self):
        try:
            self.lock = (self.root / "api-control.lock").open("r+")
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.conn = sqlite3.connect(
                (self.root / "api-control.sqlite3").resolve().as_uri() + "?mode=rw",
                uri=True, timeout=1,
            )
            self.conn.execute("PRAGMA synchronous=FULL")
            row = self.conn.execute(
                "SELECT version,account_key,policy FROM metadata WHERE id=1"
            ).fetchone()
            if row is None or row[0] != 1 or row[1] != self.key:
                raise ApiControlError("API ledger version/account mismatch")
            if json.loads(row[2]) != POLICY:
                raise ApiControlError("API ledger policy differs from this image")
            # A killed process may have sent a request but never saved its outcome.
            pending = self.conn.execute(
                "SELECT id,record_json FROM attempts WHERE finished_at IS NULL"
            ).fetchall()
            if pending:
                now = self.clock()
                with self.conn:
                    for attempt_id, raw in pending:
                        record = json.loads(raw)
                        record.update(event="pv_api_attempt_finished", outcome="unknown_after_interruption",
                                      finished_at=datetime.fromtimestamp(now, timezone.utc).isoformat())
                        self.conn.execute("UPDATE attempts SET finished_at=?,record_json=? WHERE id=?",
                                          (now, json.dumps(record), attempt_id))
                    self.conn.execute("UPDATE metadata SET blocked_until=MAX(blocked_until,?) WHERE id=1",
                                      (now + DAY,))
                for _, raw in pending:
                    self.emit({"event": "pv_api_interrupted_attempt", "run_id": self.run_id,
                               "attempt_id": json.loads(raw)["attempt_id"]})
            return self
        except Exception as exc:
            self.__exit__(None, None, None)
            if isinstance(exc, ApiControlError):
                raise
            raise ApiControlError("API ledger unavailable, corrupt, or another run is active; no calls allowed") from exc

    def __exit__(self, *_):
        if self.conn is not None:
            self.conn.close()
            self.conn = None
        if self.lock is not None:
            self.lock.close()
            self.lock = None

    def _deny(self, reason, retry_at=None):
        self.emit({"event": "pv_api_request_blocked", "run_id": self.run_id,
                   "reason": reason, "retry_at_unix": retry_at})
        raise ApiControlError(reason)

    def _check_ready(self):
        if self.conn is None:
            raise ApiControlError("API control context must be active")
        blocked_until = self.conn.execute("SELECT blocked_until FROM metadata WHERE id=1").fetchone()[0]
        if self.clock() < blocked_until:
            self._deny("shared API cooldown is active", blocked_until)

    def _count(self, endpoint, seconds):
        return self.conn.execute(
            "SELECT count(*) FROM attempts WHERE endpoint=? AND started_at>?",
            (endpoint, self.clock() - seconds),
        ).fetchone()[0]

    def _check_history_budget(self, planned_history_calls):
        self._check_ready()
        if planned_history_calls not in (1, 2):
            self._deny("invalid planned history call count")
        limit = POLICY["history_limit_24h"]
        if self.trigger != "scheduled":
            limit -= POLICY["scheduled_reserve"]
        if self._count(HISTORY, DAY) + planned_history_calls > limit:
            self._deny("rolling 24h history budget exhausted (scheduled reserve protected)")

    def preflight(self, planned_history_calls):
        """Check budget for the whole run before spending even a login request."""
        self._check_history_budget(planned_history_calls)
        if self._count("getDevList", DAY) >= POLICY["device_list_limit_24h"]:
            self._deny("rolling 24h device-list budget exhausted")
        if self._count("login", 600) >= POLICY["login_limit_10m"]:
            self._deny("login budget exhausted for the last 10 minutes")

    def begin_attempt(self, endpoint, payload):
        self._check_ready()
        if endpoint not in {"login", "getDevList", HISTORY}:
            self._deny("endpoint has no configured API policy")
        if endpoint == HISTORY:
            self._check_history_budget(1)
            last = self.conn.execute("SELECT max(started_at) FROM attempts WHERE endpoint=?",
                                     (HISTORY,)).fetchone()[0]
            if last is not None:
                delay = last + POLICY["history_spacing_seconds"] - self.clock()
                if delay > 120:
                    self._deny("clock moved backwards; manual inspection required")
                while delay > 0:
                    self.emit({"event": "pv_api_pacing", "run_id": self.run_id,
                               "endpoint": endpoint, "wait_seconds": round(delay, 3)})
                    self.sleep(delay)
                    delay = last + POLICY["history_spacing_seconds"] - self.clock()
            self._check_ready()
        elif endpoint == "login" and self._count(endpoint, 600) >= POLICY["login_limit_10m"]:
            self._deny("login budget exhausted for the last 10 minutes")
        elif endpoint == "getDevList" and self._count(endpoint, DAY) >= POLICY["device_list_limit_24h"]:
            self._deny("rolling 24h device-list budget exhausted")
        now = self.clock()
        record = {"event": "pv_api_attempt_started", "attempt_id": uuid.uuid4().hex,
                  "run_id": self.run_id, "trigger_kind": self.trigger, "endpoint": endpoint,
                  "started_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
                  "automatic_retries": 0}
        if endpoint == HISTORY:
            record.update(dev_type_id=payload["devTypeId"],
                          device_count=len(payload["devIds"].split(",")),
                          window_start_ms=payload["startTime"], window_end_ms=payload["endTime"])
        # This commit MUST precede HTTP. Unknown/failed attempts still consume budget.
        try:
            with self.conn:
                self.conn.execute("INSERT INTO attempts VALUES (?,?,?,?,NULL,?)",
                                  (record["attempt_id"], self.run_id, endpoint, now, json.dumps(record)))
        except sqlite3.Error as exc:
            raise ApiControlError("API attempt could not be recorded; no request sent") from exc
        self.emit(record)
        return record

    def finish_attempt(self, record, result):
        now = self.clock()
        completed = {**record, **result, "event": "pv_api_attempt_finished",
                     "finished_at": datetime.fromtimestamp(now, timezone.utc).isoformat()}
        cooldown = 0
        if result["outcome"] == "account_rate_limit":
            cooldown = POLICY["account_cooldown_seconds"]
        elif result["outcome"] == "system_rate_limit":
            cooldown = POLICY["system_cooldown_seconds"]
        elif result["outcome"] != "success":
            cooldown = POLICY["uncertain_cooldown_seconds"]
        cooldown = max(cooldown, result.get("retry_after_seconds") or 0)
        try:
            with self.conn:
                updated = self.conn.execute("UPDATE attempts SET finished_at=?,record_json=? WHERE id=? AND finished_at IS NULL",
                                            (now, json.dumps(completed), record["attempt_id"]))
                if updated.rowcount != 1:
                    raise ApiControlError("attempt completion was not recorded")
                if cooldown:
                    self.conn.execute("UPDATE metadata SET blocked_until=MAX(blocked_until,?) WHERE id=1",
                                      (now + cooldown,))
        except sqlite3.Error as exc:
            raise ApiControlError("API outcome could not be recorded; attempt remains pending; no further calls allowed") from exc
        self.emit(completed)
        return completed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["initialize", "status"])
    parser.add_argument("--state-dir", default=os.getenv("PV_API_STATE_DIR"))
    args = parser.parse_args()
    if not args.state_dir:
        parser.error("--state-dir or PV_API_STATE_DIR is required")
    if args.operation == "initialize":
        initialize(args.state_dir, os.getenv("FUSIONSOLAR_USERNAME", ""))
    with sqlite3.connect((Path(args.state_dir) / "api-control.sqlite3").resolve().as_uri() + "?mode=ro", uri=True) as conn:
        until, policy = conn.execute("SELECT blocked_until,policy FROM metadata WHERE id=1").fetchone()
        counts = dict(conn.execute("SELECT endpoint,count(*) FROM attempts WHERE started_at>? GROUP BY endpoint",
                                   (time.time() - DAY,)))
        pending = conn.execute("SELECT count(*) FROM attempts WHERE finished_at IS NULL").fetchone()[0]
        latest = [json.loads(row[0]) for row in conn.execute(
            "SELECT record_json FROM attempts ORDER BY started_at DESC LIMIT 10")]
        print(json.dumps({"blocked_until_utc": datetime.fromtimestamp(until, timezone.utc).isoformat(),
                          "attempts_last_24h": counts, "policy": json.loads(policy),
                          "pending_attempts": pending, "latest_attempts": latest}))


if __name__ == "__main__":
    main()
