"""Hourly Shelly counter aggregation. Does not recalculate pre-cutover history."""
import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import os
from zoneinfo import ZoneInfo

import psycopg2

from counter_energy import BOUNDARY_TOLERANCE_SECONDS, HOUR, METHOD, Sample, hourly_energy, utc

LOCAL_TZ = ZoneInfo('Europe/Athens')
RECHECK_HOURS = 3


def get_connection():
    return psycopg2.connect(host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=int(os.getenv('POSTGRES_INTERNAL_PORT', '5432')),
        dbname=os.getenv('POSTGRES_DB'), user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'))


def is_working_period(start_time_utc):
    local = start_time_utc.astimezone(LOCAL_TZ)
    day = int(local.weekday() < 5)
    return day, int(bool(day) and 8 <= local.hour < 14)


def parse_hour(value):
    result = utc(datetime.fromisoformat(value.replace('Z', '+00:00')))
    if result.minute or result.second or result.microsecond:
        raise ValueError('Expected an aligned hour with an explicit timezone offset')
    return result


def device_channels(device):
    if device.startswith('shellyplug'):
        return ('total',)
    if device.startswith('shellypro3em'):
        return ('a', 'b', 'c')
    return ()


def calculate_device(cur, device, start, end):
    """One index-bounded read per channel for the entire small replay window."""
    samples = {}
    pad = timedelta(seconds=BOUNDARY_TOLERANCE_SECONDS)
    for channel in device_channels(device):
        cur.execute('''SELECT observed_at,energy_wh,returned_energy_wh,counter_kind
            FROM shelly_energy_counters
            WHERE device_id=%s AND channel=%s AND observed_at >= %s AND observed_at <= %s
            ORDER BY observed_at''', (device, channel, start-pad, end+pad))
        samples[channel] = [Sample(*row) for row in cur.fetchall()]
    hour = start
    while hour < end:
        yield hour, {channel: hourly_energy(rows, hour, hour+HOUR)
                     for channel, rows in samples.items()}
        hour += HOUR


def persist_hour(cur, device, hour, results):
    # No rows for missing hours. Remove stale post-cutover results on replay,
    # rather than preserving a previously calculated value after a late reset.
    if all(r.energy_wh is None for r in results.values()):
        table = 'shelly_plug_hourly_energy' if 'total' in results else 'shelly_pro3em_hourly_energy'
        cur.execute(f'DELETE FROM {table} WHERE device_id=%s AND window_start=%s AND window_end=%s',
                    (device, hour, hour+HOUR))
        return
    day, working = is_working_period(hour)
    if 'total' in results:
        cur.execute('''INSERT INTO shelly_plug_hourly_energy
            (device_id,window_start,window_end,energy_wh,is_working_day,is_working_hour)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (device_id,window_start,window_end) DO UPDATE SET
            energy_wh=EXCLUDED.energy_wh,is_working_day=EXCLUDED.is_working_day,
            is_working_hour=EXCLUDED.is_working_hour,created_at=NOW()''',
            (device, hour, hour+HOUR, results['total'].energy_wh, day, working))
    else:
        phases = [results[p].energy_wh for p in 'abc']
        total = round(sum(phases), 3) if all(v is not None for v in phases) else None
        cur.execute('''INSERT INTO shelly_pro3em_hourly_energy
            (device_id,window_start,window_end,a_energy_wh,b_energy_wh,c_energy_wh,
             total_energy_wh,is_working_day,is_working_hour)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (device_id,window_start,window_end) DO UPDATE SET
            a_energy_wh=EXCLUDED.a_energy_wh,b_energy_wh=EXCLUDED.b_energy_wh,
            c_energy_wh=EXCLUDED.c_energy_wh,total_energy_wh=EXCLUDED.total_energy_wh,
            is_working_day=EXCLUDED.is_working_day,is_working_hour=EXCLUDED.is_working_hour,
            created_at=NOW()''', (device, hour, hour+HOUR, *phases, total, day, working))


def aggregate(conn, start, end, *, dry_run=False):
    """Idempotent hourly writes; keep per-device transactions short."""
    counts = Counter()
    if dry_run:
        conn.set_session(readonly=True)
    with conn:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout='15s'")
            if not dry_run:
                # Session lock complements cron flock, including manual invocations.
                cur.execute('SELECT pg_try_advisory_lock(742081507)')
                if not cur.fetchone()[0]:
                    raise RuntimeError('Another counter aggregator is running')
            cur.execute('SELECT device_id FROM shelly_devices ORDER BY device_id')
            devices = [row[0] for row in cur.fetchall() if device_channels(row[0])]
    try:
        for device in devices:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SET LOCAL statement_timeout='15s'")
                    for hour, results in calculate_device(cur, device, start, end):
                        counts.update(result.reason for result in results.values())
                        if not dry_run:
                            persist_hour(cur, device, hour, results)
    finally:
        if not dry_run:
            with conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT pg_advisory_unlock(742081507)')
    return dict(counts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--start', type=parse_hour)
    parser.add_argument('--end', type=parse_hour)
    args = parser.parse_args()
    now = datetime.now(timezone.utc)
    last_closed = now.replace(minute=0, second=0, microsecond=0)
    # Wait for the accepted receipt-time tolerance around the ending boundary.
    if now-last_closed < timedelta(seconds=BOUNDARY_TOLERANCE_SECONDS):
        last_closed -= HOUR
    if bool(args.start) != bool(args.end):
        parser.error('--start and --end must be supplied together')
    configured = os.getenv('SHELLY_COUNTER_START')
    if not configured and not args.dry_run:
        parser.error('SHELLY_COUNTER_START is required before enabling writes')
    cutover = parse_hour(configured) if configured else None
    end = args.end or last_closed
    start = args.start or max(last_closed-RECHECK_HOURS*HOUR, cutover or last_closed-RECHECK_HOURS*HOUR)
    if start >= end and args.start is None and cutover and cutover >= end:
        print(json.dumps({'method': METHOD, 'status': 'waiting_for_cutover'}))
        return
    if not start < end <= last_closed or end-start > timedelta(days=7):
        parser.error('Use 1 hour through 7 days of fully closed hours')
    if not args.dry_run and start < cutover:
        parser.error('Refusing to overwrite pre-cutover historical values')
    conn = get_connection()
    try:
        counts = aggregate(conn, start, end, dry_run=args.dry_run)
    finally:
        conn.close()
    print(json.dumps({'method': METHOD, 'dry_run': args.dry_run,
                      'start': start.isoformat(), 'end': end.isoformat(), 'channels': counts}))


if __name__ == '__main__':
    main()
