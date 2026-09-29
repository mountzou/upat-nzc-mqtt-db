"""Read-only polling and per-account receipts; never evaluate IAQ here."""
from contextlib import closing

from database import get_connection


def list_notifications(user, *, limit=20, before_id=None):
    # Apply current school authorization even to historical records and counts.
    scope = None if user.role == 'system_admin' else list(user.school_ids)
    with closing(get_connection()) as conn, conn:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '5s'")
            cur.execute('''SELECT n.*, r.read_at
                FROM iaq_notifications n
                LEFT JOIN iaq_notification_reads r ON r.notification_id=n.id AND r.username=%s
                WHERE (%s::text[] IS NULL OR n.school_id=ANY(%s::text[]))
                  AND (%s::bigint IS NULL OR n.id < %s)
                ORDER BY n.id DESC LIMIT %s''',
                (user.username, scope, scope, before_id, before_id, limit + 1))
            rows = cur.fetchall()
            cur.execute('''SELECT COUNT(*) AS count FROM iaq_notifications n
                WHERE (%s::text[] IS NULL OR n.school_id=ANY(%s::text[]))
                  AND NOT EXISTS (SELECT 1 FROM iaq_notification_reads r
                                  WHERE r.notification_id=n.id AND r.username=%s)''',
                (scope, scope, user.username))
            unread = cur.fetchone()['count']
    return {'items': rows[:limit], 'unread_count': unread,
            'next_before_id': rows[limit-1]['id'] if len(rows) > limit else None}


def mark_read(user, notification_id):
    scope = None if user.role == 'system_admin' else list(user.school_ids)
    with closing(get_connection()) as conn, conn:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '5s'")
            cur.execute('''INSERT INTO iaq_notification_reads (notification_id, username)
                SELECT id, %s FROM iaq_notifications
                WHERE id=%s AND (%s::text[] IS NULL OR school_id=ANY(%s::text[]))
                ON CONFLICT (username, notification_id) DO UPDATE
                    SET read_at=iaq_notification_reads.read_at
                RETURNING notification_id, read_at''',
                (user.username, notification_id, scope, scope))
            return cur.fetchone()
