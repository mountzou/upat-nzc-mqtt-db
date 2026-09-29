import argparse
import json
import math
import os
from collections import Counter
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg2

LOCAL_TZ = ZoneInfo(os.getenv("OPEN_METEO_TIMEZONE", "Europe/Athens"))
RECHECK_HOURS = 3
HOUR = timedelta(hours=1)
BOUNDARY_TOLERANCE = timedelta(seconds=60)
ADVISORY_LOCK_ID = 742081507


@dataclass(frozen=True)
class Sample:
    observed_at: datetime
    energy_wh: float
    returned_energy_wh: float | None
    counter_kind: str


@dataclass(frozen=True)
class Result:
    energy_wh: float | None
    reason: str


def validate_samples(samples):
    counter_kind = samples[0].counter_kind
    has_returned_energy = samples[0].returned_energy_wh is not None

    for sample in samples:
        if (
            sample.counter_kind != counter_kind
            or (sample.returned_energy_wh is not None) != has_returned_energy
        ):
            return "counter_shape_changed"
        if counter_kind not in ("import", "absolute"):
            return "invalid_counter"

        values = [sample.energy_wh]
        if has_returned_energy:
            values.append(sample.returned_energy_wh)
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
            for value in values
        ):
            return "invalid_counter"
        if (
            counter_kind == "absolute"
            and has_returned_energy
            and sample.returned_energy_wh > sample.energy_wh
        ):
            return "invalid_counter"

    return None


def get_hourly_samples(samples, start, end):
    start = start.astimezone(timezone.utc)
    end = end.astimezone(timezone.utc)
    if end - start != HOUR or start.minute or start.second or start.microsecond:
        raise ValueError("Expected one aligned elapsed hour")

    samples_by_time = {}
    conflicts = set()
    for sample in samples:
        timestamp = sample.observed_at.astimezone(timezone.utc)
        if not start - BOUNDARY_TOLERANCE <= timestamp <= end + BOUNDARY_TOLERANCE:
            continue
        if timestamp in samples_by_time and samples_by_time[timestamp] != sample:
            conflicts.add(timestamp)
        samples_by_time[timestamp] = sample

    timestamps = sorted(samples_by_time)
    if len(timestamps) < 2:
        return [], "insufficient_samples"

    first = min(timestamps, key=lambda timestamp: (abs(timestamp - start), timestamp))
    last = min(timestamps, key=lambda timestamp: (abs(timestamp - end), timestamp))
    if abs(first - start) > BOUNDARY_TOLERANCE or abs(last - end) > BOUNDARY_TOLERANCE:
        return [], "missing_boundary"

    selected = [timestamp for timestamp in timestamps if first <= timestamp <= last]
    if conflicts.intersection(selected):
        return [], "conflicting_timestamp"

    hourly_samples = [samples_by_time[timestamp] for timestamp in selected]
    reason = validate_samples(hourly_samples)
    if reason:
        return [], reason
    return hourly_samples, None


def pro3em_hourly_energy(samples, start, end):
    hourly_samples, reason = get_hourly_samples(samples, start, end)
    if reason:
        return Result(None, reason)
    if hourly_samples[0].counter_kind != "import":
        return Result(None, "invalid_counter")

    for previous, current in zip(hourly_samples, hourly_samples[1:]):
        if (
            current.energy_wh < previous.energy_wh
            or (
                previous.returned_energy_wh is not None
                and current.returned_energy_wh < previous.returned_energy_wh
            )
        ):
            return Result(None, "counter_reset")

    energy_wh = hourly_samples[-1].energy_wh - hourly_samples[0].energy_wh
    return Result(round(energy_wh, 3), "observed")


def plug_hourly_energy(samples, start, end):
    hourly_samples, reason = get_hourly_samples(samples, start, end)
    if reason:
        return Result(None, reason)
    if hourly_samples[0].counter_kind != "absolute":
        return Result(None, "invalid_counter")

    for previous, current in zip(hourly_samples, hourly_samples[1:]):
        energy_delta = current.energy_wh - previous.energy_wh
        returned_delta = (current.returned_energy_wh or 0) - (previous.returned_energy_wh or 0)
        if energy_delta < 0 or returned_delta < 0:
            return Result(None, "counter_reset")
        if energy_delta < returned_delta:
            return Result(None, "invalid_import_delta")

    first, last = hourly_samples[0], hourly_samples[-1]
    energy_wh = last.energy_wh - first.energy_wh
    returned_energy_wh = (last.returned_energy_wh or 0) - (first.returned_energy_wh or 0)
    return Result(round(energy_wh - returned_energy_wh, 3), "observed")


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_INTERNAL_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def device_channels(device_id):
    if device_id.startswith("shellyplug"):
        return ("total",)
    if device_id.startswith("shellypro3em"):
        return ("a", "b", "c")
    return ()


def get_device_samples(cursor, device_id, start, end):
    channel_samples = {}
    for channel in device_channels(device_id):
        cursor.execute(
            """
            SELECT observed_at, energy_wh, returned_energy_wh, counter_kind
            FROM shelly_energy_counters
            WHERE device_id = %s AND channel = %s
              AND observed_at >= %s AND observed_at <= %s
            ORDER BY observed_at
            """,
            (device_id, channel, start - BOUNDARY_TOLERANCE, end + BOUNDARY_TOLERANCE),
        )
        channel_samples[channel] = [Sample(*row) for row in cursor.fetchall()]
    return channel_samples


def calculate_device(cursor, device_id, start, end):
    if device_id.startswith("shellyplug"):
        calculate_energy = plug_hourly_energy
    else:
        calculate_energy = pro3em_hourly_energy

    channel_samples = get_device_samples(cursor, device_id, start, end)
    hour = start
    while hour < end:
        results = {
            channel: calculate_energy(samples, hour, hour + HOUR)
            for channel, samples in channel_samples.items()
        }
        yield hour, results
        hour += HOUR


def persist_hour(cursor, device_id, hour, results):
    is_plug = "total" in results
    if all(result.energy_wh is None for result in results.values()):
        # Replays remove a stale result when a late reset makes the hour unusable.
        table = "shelly_plug_hourly_energy" if is_plug else "shelly_pro3em_hourly_energy"
        cursor.execute(
            f"DELETE FROM {table} WHERE device_id = %s AND window_start = %s AND window_end = %s",
            (device_id, hour, hour + HOUR),
        )
        return

    local_hour = hour.astimezone(LOCAL_TZ)
    working_day = int(local_hour.weekday() < 5)
    working_hour = int(working_day and 8 <= local_hour.hour < 14)
    if is_plug:
        cursor.execute(
            """
            INSERT INTO shelly_plug_hourly_energy (
                device_id, window_start, window_end, energy_wh,
                is_working_day, is_working_hour
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (device_id, window_start, window_end) DO UPDATE SET
                energy_wh = EXCLUDED.energy_wh,
                is_working_day = EXCLUDED.is_working_day,
                is_working_hour = EXCLUDED.is_working_hour,
                created_at = NOW()
            """,
            (device_id, hour, hour + HOUR, results["total"].energy_wh, working_day, working_hour),
        )
        return

    phases = [results[phase].energy_wh for phase in ("a", "b", "c")]
    total = round(sum(phases), 3) if all(value is not None for value in phases) else None
    cursor.execute(
        """
        INSERT INTO shelly_pro3em_hourly_energy (
            device_id, window_start, window_end, a_energy_wh, b_energy_wh,
            c_energy_wh, total_energy_wh, is_working_day, is_working_hour
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (device_id, window_start, window_end) DO UPDATE SET
            a_energy_wh = EXCLUDED.a_energy_wh,
            b_energy_wh = EXCLUDED.b_energy_wh,
            c_energy_wh = EXCLUDED.c_energy_wh,
            total_energy_wh = EXCLUDED.total_energy_wh,
            is_working_day = EXCLUDED.is_working_day,
            is_working_hour = EXCLUDED.is_working_hour,
            created_at = NOW()
        """,
        (device_id, hour, hour + HOUR, *phases, total, working_day, working_hour),
    )


def aggregate(conn, start, end, *, dry_run=False):
    counts = Counter()
    if dry_run:
        conn.set_session(readonly=True)

    with conn, conn.cursor() as cursor:
        cursor.execute("SET LOCAL statement_timeout = '15s'")
        if not dry_run:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_ID,))
            if not cursor.fetchone()[0]:
                raise RuntimeError("Another counter aggregator is running")
        cursor.execute("SELECT device_id FROM shelly_devices ORDER BY device_id")
        devices = [row[0] for row in cursor.fetchall() if device_channels(row[0])]

    try:
        for device_id in devices:
            with conn, conn.cursor() as cursor:
                cursor.execute("SET LOCAL statement_timeout = '15s'")
                for hour, results in calculate_device(cursor, device_id, start, end):
                    counts.update(result.reason for result in results.values())
                    if not dry_run:
                        persist_hour(cursor, device_id, hour, results)
    finally:
        if not dry_run:
            with conn, conn.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_ID,))

    return dict(counts)


def parse_hour(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("An explicit timezone offset is required")
    parsed = parsed.astimezone(timezone.utc)
    if parsed.minute or parsed.second or parsed.microsecond:
        raise ValueError("Expected an aligned hour with an explicit timezone offset")
    return parsed


def get_aggregation_window(args, parser):
    if (args.start is None) != (args.end is None):
        parser.error("--start and --end must be supplied together")

    configured_start = os.getenv("SHELLY_COUNTER_START")
    if not configured_start and not args.dry_run:
        parser.error("SHELLY_COUNTER_START is required before enabling writes")
    cutover = parse_hour(configured_start) if configured_start else None

    now = datetime.now(timezone.utc)
    last_closed = now.replace(minute=0, second=0, microsecond=0)
    if now - last_closed < BOUNDARY_TOLERANCE:
        last_closed -= HOUR

    default_start = last_closed - RECHECK_HOURS * HOUR
    if cutover:
        default_start = max(default_start, cutover)
    start = args.start or default_start
    end = args.end or last_closed

    if args.start is None and cutover is not None and cutover >= end:
        return None
    if not start < end <= last_closed or end - start > timedelta(days=7):
        parser.error("Use 1 hour through 7 days of fully closed hours")
    if not args.dry_run and start < cutover:
        parser.error("Refusing to overwrite pre-cutover historical values")
    return start, end


def main():
    parser = argparse.ArgumentParser(description="Aggregate hourly energy from Shelly counters.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--start", type=parse_hour)
    parser.add_argument("--end", type=parse_hour)
    args = parser.parse_args()

    window = get_aggregation_window(args, parser)
    if window is None:
        print(json.dumps({"status": "waiting_for_cutover"}))
        return
    start, end = window

    with closing(get_connection()) as conn:
        counts = aggregate(conn, start, end, dry_run=args.dry_run)
    print(json.dumps({
        "dry_run": args.dry_run,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "channels": counts,
    }))


if __name__ == "__main__":
    main()
