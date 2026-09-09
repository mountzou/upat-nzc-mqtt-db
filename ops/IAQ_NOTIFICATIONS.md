# Room IAQ notifications — local PoC

This change is local implementation only. No migration or timer has been applied
on the VPS. Frontend integration is separate.

## Contract

All rooms in `api/monitoring/catalog/device_assignments.json` are checked.
Room identity is `(school_id, room_id)`. Notifications are never school-level
summaries. Single-sensor rooms use the stored hourly `value_avg` directly.
For School 10 Teachers Office, portable-110 and portable-111 represent two
subspaces: the room uses the simple arithmetic mean of their available sensor
means, with equal sensor weights. If one is missing use the other; if both are
missing produce no notification for that metric.

Daily: compute each sensor's daily mean from its hourly aggregates, weighted by
`sample_count`, then apply the same equal-sensor room mean. The persisted total
sample count is evidence of available data, not a weight between the subspaces.

| Job | Athens schedule | Period | Match |
| --- | --- | --- | --- |
| hourly | Every hour at :10 | Previous complete clock hour | CO2 > short threshold; PM2.5 > short threshold |
| daily | Every day at 00:10 | Previous calendar day | CO2 < long threshold; PM2.5 < long threshold |

Thresholds come directly from `monitoring/policies/iaq.py`: currently CO2
750 ppm hourly / 800 ppm daily and PM2.5 10 / 15 µg/m³. Equality does not match.
Calendar days use midnight-to-midnight Athens bounds, naturally including the
previous day's 23:59 minute. Event timestamps are stored with timezone.

Missing/null/non-finite readings do not contribute; no samples for a metric
means no notification for that metric. A valid measured zero is not missing.
This first version has no minimum sample-coverage rule: a daily good notification
only describes the mean of available samples, not uninterrupted good conditions.
The sample count and exact period are included in its persisted evidence.

Hourly matches in consecutive hours produce separate notifications. CO2 and
PM2.5 can both produce notifications for the same room/period. A daily good mean
can coexist with hourly elevated means.

## Generation and persistence

`python -m monitoring.notifications.job hourly` and `... daily` are short-lived
processes, independent of HTTP requests. They read only
`upat_measurements_hourly`. Hourly checks select the stored average directly;
daily checks use `SUM(value_avg * sample_count) / SUM(sample_count)` per sensor.
Room means use `AVG(sensor_average)`; no raw-data fallback.
The live VPS crontab was inspected read-only on 9 September 2026: incremental
rollups run every five minutes (`*/5`). Hourly notifications run at :10, ten
minutes after the :00 rollup, and evaluate the previous completed hour. This
is a scheduling margin, not a guarantee against failed or delayed rollup jobs.

These commands evaluate the previous period at execution time, not arbitrary
historical backfills. Late data arriving after a successful check does not
retroactively rewrite that check's history.

Migration `db/migrations/016_iaq_notifications.sql` adds:

- `iaq_notifications`: permanent history and threshold/value/period evidence.
- `iaq_notification_reads`: independent read timestamp per account.
- `iaq_notification_runs`: successful period checks, including zero matches.

A transaction-level advisory lock per period kind prevents overlapping same-kind jobs.
Hourly and daily checks can run independently at 00:10. Notifications and
the successful run marker commit together. A failure rolls everything back;
retry within the same period is safe. A completed period is skipped on rerun.
A database unique constraint additionally protects room/metric/period identity.
No automatic history deletion or episode/recovery mechanism is included.

The `--dry-run` flag evaluates the same transaction and rolls it back (sequence
IDs may be consumed). It needs the migration already applied. JSON output reports
period, room count, observed room/metric count and notification count. Errors
exit nonzero and appear in the process journal. Missing schema is an error, not
an empty successful check.

## Authenticated API

- `GET /notifications?limit=20`: newest entries, `unread_count`, `next_before_id`.
- `GET /notifications?limit=20&before_id=123`: older history (keyset pagination).
- `PATCH /notifications/123/read`: mark read; repeat calls keep the first timestamp.

GET never evaluates sensors or generates notifications. It is suitable for one
application-wide poll every two minutes. Responses use `Cache-Control: no-store`.
Counts span the authorized history, not just the current page. Polling the first
page also reconciles reads made in other tabs; the frontend can fetch older pages
when opening history.

The existing authenticated user's **current** school scope filters both reads
and updates in SQL. A school account sees only its school. Municipality/admin
accounts retain their existing authorized scope; these are still room events.
Unauthorized IDs return 404. Read state belongs to the account, so people sharing
an account share read state. Authorized accounts can see older school history,
including events predating account creation. Losing school access hides its
history immediately on subsequent requests.

## Local validation and eventual deployment

Use a disposable local PostgreSQL with the tests in
`api/tests/test_iaq_notifications.py`; never point fixture tests at production.
The tests require `NOTIFICATIONS_TEST_DSN` containing `host=127.0.0.1` and
`local-fixture-only`. They create and remove only their fixture schema.

For eventual approved VPS rollout: apply migration 016, deploy the reviewed API
image (the job is packaged in the same image), then install the three
`ops/systemd/upat-iaq-notifications*` templates. They execute a separate Python
process in the API container; no background task is attached to Uvicorn startup.
Confirm Docker path, Compose project/location and API image before installation.
Timers are **not installed or enabled by this change**. They do not catch up all
missed periods after downtime; this PoC evaluates only scheduled previous periods.
A later recovery/backfill policy can be added explicitly if needed.

Verification must check job exit, run marker, notification evidence and API scope.
An empty notification list alone cannot distinguish healthy conditions from a
failed scheduler; inspect `iaq_notification_runs` and the journal.

For a local test container whose network namespace is shared with the disposable
PostgreSQL container, mount this worktree at `/workspace` and run from
`/workspace/api` with `PYTHONPATH=/workspace/api`:

```sh
python -m pytest -p no:cacheprovider tests/test_iaq_notifications.py tests/test_monitoring_catalog.py tests/test_monitoring_contract_iaq_policy.py -q
```

### Initial raw-reader verification — 9 September 2026 (before aggregate-only change)

- 32 tests passed across notifications, catalog authorization and IAQ policy.
- SQL tests exercised real disposable PostgreSQL 15: room sample weighting,
  strict thresholds, missing data, consecutive hours, per-account reads,
  school isolation, duplicate prevention, overlapping jobs and transaction
  rollback after notification insertion.
- Full `db/init.sql` initialization, including migration 016, succeeded in the
  disposable database; repeated application of migration 016 also passed.
- The unchanged API Dockerfile built successfully with the new modules included.
- No production database, scheduler or frontend was changed.

### Aggregate-only update — 9 September 2026

The notification job now reads hourly aggregates exclusively. The hourly timer
is :10 and the daily timer remains 00:10 Athens time. Tests use a database with
only an hourly-aggregate table (no raw table), and include weighted daily
buckets plus independent hourly/daily execution at 00:10. Earlier School 10
raw-reader validation remains historical evidence, not a rerun of this version.

Validation of the aggregate-only update: **34 tests passed**, including real
PostgreSQL SQL/API tests, catalog scope regressions and policy contracts.
`git diff --check` passed. The timer changes remain local and uninstalled.

### Final room-mean rule — 9 September 2026

Finalized equal weighting of the two Teachers Office sensor means for both
hourly and daily notifications. Single-sensor hourly checks preserve the stored
`value_avg`; daily sensor means retain sample-weighted hourly aggregation.
One available sensor is sufficient; neither available means no notification.

**36 tests passed** against the final code, including real PostgreSQL tests
with no raw telemetry table. Added checks distinguish the simple two-sensor
mean from sample-weighted pooling, verify missing-sensor behavior, and preserve
a stored hourly value immediately above its threshold without rounding.

The earlier School 10 real-data report predates this final Teachers Office
rule and must not be used as final-version production acceptance evidence.
