# Simulation recorder job compatibility

The recorder submits one `POST /simulate/day-ahead` per admitted school/date attempt.
It continues to accept the existing synchronous HTTP 200 day-ahead payload, so
this caller can be released before the backend job migration.

When the backend returns HTTP 504 with `detail.code=simulation_wait_timeout`, the
recorder validates and commits `run_id` before polling the canonical same-origin
`status_url` using `result_format=day-ahead`. The backend owns that projection;
no energy aggregation is duplicated here. Only GET requests are retried. POST
redirects and automatic POST retries are disabled, including for HTTP 5xx.

The backend's HTTP 502/503 `simulation_failed` and `simulation_worker_unavailable`
responses are terminal. Polling failures, an exhausted polling budget, or a
transport error without a confirmed handle retain an unresolved attempt. A
later recorder invocation for that same school and target date resumes a saved
pending handle. A terminal failure or an unknown submission without a handle
requires explicit reconciliation; it never causes another automatic POST.
This is not server-side idempotency: concurrent recorder processes must still
use the existing cron `flock`.

## Persistence and quality

No database migration is needed. Existing `simulation_day_ahead_runs` columns
store the external handle, target date, `pending`/`submission_unknown` status and
response. Admission is committed before sending the POST; known handles are
committed before polling or result writes. Final run and room results commit
atomically. A failed final transaction leaves the durable handle available to
resume. A crash before the POST can therefore leave an unknown attempt requiring
manual reconciliation; this deliberately avoids guessing whether work started.

Only `success` or `completed_with_warnings` with matching school/date/run,
all requested rooms successful, and a complete, contiguous day-ahead hourly
profile can be recorded as successful. The profile must match the facility total.
Observed zero is valid; missing optional metrics remain null. Legacy HTTP 200
`partial_success` is rejected. Historical rows are not rewritten.

## Configuration

- `SIMULATION_CONNECT_TIMEOUT_SECONDS`: 10 (existing POST connection timeout).
- `SIMULATION_REQUEST_TIMEOUT_SECONDS`: 600 (existing POST read timeout).
- `SIMULATION_POLL_INTERVAL_SECONDS`: 5.
- `SIMULATION_POLL_REQUEST_TIMEOUT_SECONDS`: 30 (per status GET).
- `SIMULATION_POLL_TIMEOUT_SECONDS`: 1800 (polling budget after receiving a handle).

`SIMULATION_REQUEST_RETRIES`, `SIMULATION_REQUEST_RETRY_DELAY_SECONDS` and
`SIMULATION_REQUEST_MAX_RETRY_DELAY_SECONDS` are retired. GET retries are bounded
by the polling budget. An expired token or unavailable/expired job stops polling
and retains the handle for reconciliation. The next invocation authenticates
again. Target dates are still tomorrow in Europe/Athens; old pending dates are
not automatically replayed by the next day's schedule.

## Validation and release order

Run the recorder unit suite with `python -m pytest test_main.py test_job_client.py`.
Cross-repository tests live in EnergyPlus at
`backend/tests/simulation/test_recorder_job_integration.py`. They require:

- `SIMULATION_RECORDER_DIR`: absolute path to this directory.
- `SIMULATION_RECORDER_SCHEMA`: absolute path to `db/migrations/002_simulation_day_ahead_results.sql`.
- `CALLER_TEST_DATABASE_DSN`: a disposable localhost PostgreSQL database named `caller_test`.

The tests create a separate temporary schema per case. They connect the real
caller to FastAPI with a mocked engine, exercise the actual job queue and SQL
persistence, and never call weather or EnergyPlus providers.

## Review and integration status (2026-09-29)

The EnergyPlus backend job patch and the cross-repository integration test are
already in its main branch (commit e249f52). The integration suite passed 13 tests
against a disposable local PostgreSQL database, and this recorder's local suite
passed 45 tests. Those tests do not verify the live backend deployment.

Git integration order: reconcile PostgreSQL/Shelly with PR #4, then merge this
recorder PR after the calendar-day blocker below is resolved. The combined
production Compose matches the installed VPS file byte-for-byte. Build any
future recorder image with both `main.py` and `job_client.py`, then verify
its source/image identity and the deployed backend version before a separately
authorized rollout. Existing cron time, six-school scope and PostgreSQL service
remain unchanged. The caller accepts both the old synchronous response and the
new job response. A backend rollback to synchronous responses does not resolve
a previously saved pending job; reconcile those handles separately.

**Calendar-day blocker:** this recorder validates 23, 24, or 25 contiguous
elapsed one-hour intervals according to the length of the target Athens day.
The actual backend forecast path rejects 2026-10-25 while preparing its fixed
timezone EPW, before EnergyPlus runs. Separately, a synthetic cross-repository
projection check accepted 2026-09-08 but rejected 2026-03-29 and 2026-10-25:
the backend marked 24 local-clock intervals complete although those days have
23 and 25 elapsed hours. The backend weather and EnergyPlus output mapping
still need one shared DST-day policy before this PR is merged.
