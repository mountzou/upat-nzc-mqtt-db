"""One-shot scheduled evaluation: python -m monitoring.notifications.job hourly|daily."""
import argparse
from contextlib import closing
from datetime import datetime, time, timedelta, timezone
import json
from zoneinfo import ZoneInfo

from psycopg2.extras import Json

from database import get_connection
from monitoring.config import APP_TIMEZONE_NAME
from monitoring.policies.iaq import IAQ_POLICY_VERSION, get_iaq_metric_descriptor, get_iaq_threshold
from monitoring.services.environment.devices import _load_environment_device_catalog

ATHENS = ZoneInfo(APP_TIMEZONE_NAME)


def previous_period(kind, now):
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError('An aware execution time is required')
    local = now.astimezone(ATHENS)
    if kind == 'hourly':
        end = local.replace(minute=0, second=0, microsecond=0).astimezone(timezone.utc)
        return end - timedelta(hours=1), end
    if kind == 'daily':
        end = datetime.combine(local.date(), time.min, ATHENS)
        start = datetime.combine(local.date() - timedelta(days=1), time.min, ATHENS)
        return start.astimezone(timezone.utc), end.astimezone(timezone.utc)
    raise ValueError('Unknown period kind')


def room_assignments():
    assignments = []
    seen = set()
    for school in _load_environment_device_catalog():
        for device in school.devices:
            if device.id in seen:
                raise ValueError('Environmental device is assigned more than once')
            seen.add(device.id)
            assignments.append(dict(school_id=school.school_id, room_id=device.room_id,
                                    room_label=device.label, device_id=device.id))
    return assignments


def read_averages(cur, assignments, start, end, kind):
    # Hourly: read the stored mean unchanged. Daily: combine hours per sensor.
    # Then give each available sensor equal weight within its room (teachers).
    sensor_projection = (
        'metric, value_avg AS average, sample_count AS n'
        if kind == 'hourly' else
        'metric, SUM(value_avg * sample_count) / SUM(sample_count) AS average, SUM(sample_count) AS n'
    )
    sensor_grouping = '' if kind == 'hourly' else 'GROUP BY metric'
    # Only the trusted projections above are interpolated; all data is bound.
    cur.execute(f'''
        WITH assignments AS (
            SELECT * FROM jsonb_to_recordset(%s::jsonb)
            AS a(school_id text, room_id text, room_label text, device_id text)
        )
        SELECT a.school_id, a.room_id, MIN(a.room_label) AS room_label, m.metric,
               AVG(m.average) AS average, SUM(m.n)::bigint AS sample_count
        FROM assignments a CROSS JOIN LATERAL (
            SELECT {sensor_projection}
            FROM upat_measurements_hourly
            WHERE device_id = a.device_id AND metric IN ('co2', 'pm25')
              AND bucket_start >= %s AND bucket_start < %s
              AND sample_count > 0
              AND value_avg > '-Infinity'::float8 AND value_avg < 'Infinity'::float8
            {sensor_grouping}
        ) m
        GROUP BY a.school_id, a.room_id, m.metric
        ORDER BY a.school_id, a.room_id, m.metric
    ''', (Json(assignments), start, end))
    return cur.fetchall()


def notification(row, kind, start, end):
    threshold = get_iaq_threshold(row['metric'], 'short' if kind == 'hourly' else 'long')
    average = row['average']
    matches = average > threshold if kind == 'hourly' else average < threshold
    if not matches:
        return None
    descriptor = get_iaq_metric_descriptor(row['metric'])
    status = 'Elevated' if kind == 'hourly' else 'Good'
    comparison = 'above' if kind == 'hourly' else 'below'
    return {**row, 'period_kind': kind, 'period_start': start, 'period_end': end,
            'threshold': threshold, 'policy_version': IAQ_POLICY_VERSION,
            'unit': descriptor['unit'],
            'title': f"{status} {descriptor['label']} — {row['room_label']}",
            'body': f"The {kind} average was {average:.1f} {descriptor['unit']}, "
                    f"{comparison} the configured {kind} threshold of {threshold:g} {descriptor['unit']}."}


def run(kind, *, now=None, dry_run=False, connection_factory=get_connection):
    start, end = previous_period(kind, now or datetime.now(timezone.utc))
    assignments = room_assignments()
    result = dict(period_kind=kind, period_start=start.isoformat(), period_end=end.isoformat(),
                  dry_run=dry_run, room_count=len({(a['school_id'], a['room_id']) for a in assignments}))
    with closing(connection_factory()) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '30s'")
                # Serialize same-kind runs. Hourly and daily may both run at 00:10.
                cur.execute('SELECT pg_try_advisory_xact_lock(74201916, %s) AS acquired',
                            (1 if kind == 'hourly' else 2,))
                if not cur.fetchone()['acquired']:
                    raise RuntimeError('Another IAQ notification check is running')
                cur.execute('SELECT notification_count FROM iaq_notification_runs WHERE period_kind=%s AND period_start=%s', (kind, start))
                existing = cur.fetchone()
                if existing:
                    conn.rollback()
                    return {**result, 'status': 'already_checked', 'notification_count': existing['notification_count']}
                rows = read_averages(cur, assignments, start, end, kind)
                items = [item for row in rows if (item := notification(row, kind, start, end))]
                inserted = 0
                for item in items:
                    cur.execute('''INSERT INTO iaq_notifications
                        (school_id, room_id, room_label, metric, period_kind, period_start, period_end,
                         average, sample_count, threshold, policy_version, unit, title, body)
                        VALUES (%(school_id)s, %(room_id)s, %(room_label)s, %(metric)s, %(period_kind)s,
                                %(period_start)s, %(period_end)s, %(average)s, %(sample_count)s,
                                %(threshold)s, %(policy_version)s, %(unit)s, %(title)s, %(body)s)
                        ON CONFLICT (school_id, room_id, metric, period_kind, period_start) DO NOTHING
                    ''', item)
                    inserted += cur.rowcount
                cur.execute('''INSERT INTO iaq_notification_runs
                    (period_kind, period_start, period_end, room_count, observed_metric_count, notification_count)
                    VALUES (%s,%s,%s,%s,%s,%s)''',
                    (kind, start, end, result['room_count'], len(rows), inserted))
            if dry_run:
                conn.rollback()
            else:
                conn.commit()
            return {**result, 'status': 'preview' if dry_run else 'completed',
                    'observed_metric_count': len(rows), 'notification_count': inserted}
        except Exception:
            conn.rollback()
            raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('period', choices=['hourly', 'daily'])
    parser.add_argument('--dry-run', action='store_true', help='Evaluate and roll back all writes')
    args = parser.parse_args()
    print(json.dumps(run(args.period, dry_run=args.dry_run)))


if __name__ == '__main__':
    main()
