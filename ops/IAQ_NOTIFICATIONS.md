# Room IAQ notifications

The VPS notification schema is installed and the hourly/daily timers are enabled.
Jobs run as separate Python processes in the API container, independently of
HTTP requests and Uvicorn startup.

## Contract

All rooms assigned in the `environment` section of `api/monitoring/catalog/device_assignments.json` are checked.
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
| hourly | Every hour at :10 | Previous complete clock hour | CO2 > `1h` threshold; PM2.5 > `1h` threshold |
| daily | Every day at 00:10 | Previous calendar day | CO2 < `24h` threshold; PM2.5 < `24h` threshold |

Thresholds come directly from `monitoring/policies/iaq.py`: currently CO2
750 ppm hourly / 800 ppm daily and PM2.5 10 / 15 µg/m³. Equality does not match.
Calendar days use midnight-to-midnight Athens bounds, naturally including the
previous day's 23:59 minute. Event timestamps are stored with timezone.

Missing/null/non-finite readings do not contribute; no samples for a metric
means no notification for that metric. A valid measured zero is not missing.
There is no minimum sample-coverage rule: a daily good notification
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
The :10 schedule allows time for hourly rollups. It is a scheduling margin,
not a guarantee that a failed or delayed rollup is ready.

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

## Operation and recovery

The maintained units are `upat-iaq-notifications@.service`,
`upat-iaq-notifications-hourly.timer` and `upat-iaq-notifications-daily.timer`.
They evaluate the previous period at execution time, without catching up every
missed period after downtime. Migration 016 is a schema prerequisite for a new
installation; the existing VPS schema does not need initial setup repeated.

```sh
systemctl status upat-iaq-notifications-hourly.timer upat-iaq-notifications-daily.timer --no-pager
journalctl -u upat-iaq-notifications@hourly.service -u upat-iaq-notifications@daily.service -n 50 --no-pager
```

Check job exit, `iaq_notification_runs`, persisted evidence and API scope.
An empty notification list alone cannot distinguish zero matches from a failed
scheduler. API recovery must retain the notification routes, job module and
compatible tables; coordinate timers with any reviewed image change. Preserve
existing notification/read history and the [current storage/time contract](shelly-compact/RECOVERY.md).

## Local validation

Use an API dependency environment and disposable local PostgreSQL.
[Test fixtures](../api/tests/test_iaq_notifications.py) require
`NOTIFICATIONS_TEST_DSN` containing `host=127.0.0.1` and `local-fixture-only`;
they create/remove only their fixture schema. Historical test counts and
intermediate calculation-rule reports remain in Git history.
