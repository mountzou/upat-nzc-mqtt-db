#!/usr/bin/env python3
"""Explicit, resumable copy and full verification. Never drops a source table."""

import argparse
import hashlib
import json
import os
import time
from contextlib import closing
from pathlib import Path

import psycopg2
from psycopg2.extensions import parse_dsn

ROOT = Path(__file__).resolve().parents[2]
FIELDS = "id,device_id,metric,value,unit,event_time"
SOURCES = {
    "forward": "public.shelly_measurements",
    "reverse": "shelly_compact.readings",
}


def prepare(conn):
    with conn, conn.cursor() as cur:
        cur.execute((ROOT / "db/migrations/017_shelly_compact_prepare.sql").read_text())


def capture(conn, direction):
    # Operator must pause/drain writers before this checkpoint. The lock also
    # waits for prior source transactions, so out-of-order commits cannot be skipped.
    table = (
        "public.shelly_measurements"
        if direction == "forward"
        else "shelly_compact.measurements"
    )
    with conn, conn.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout='1s'; SET LOCAL statement_timeout='5s'")
        cur.execute(f"LOCK TABLE {table} IN SHARE MODE")
        cur.execute(f"SELECT coalesce(max(id),-2147483648) FROM {table}")
        high = cur.fetchone()[0]
        cur.execute(
            """INSERT INTO shelly_compact.copy_progress(direction,high_water)
            VALUES (%s,%s) ON CONFLICT(direction) DO UPDATE SET
            high_water=EXCLUDED.high_water,updated_at=now()
            WHERE copy_progress.last_id<=EXCLUDED.high_water RETURNING high_water""",
            (direction, high),
        )
        if cur.fetchone() is None:
            raise ValueError("Source watermark moved backwards")
    return high


def verify_range(cur, lo, hi):
    # Binary field comparison retains signed zero and the original float8 bits.
    bits = lambda a: ",".join(
        f"pg_catalog.{fn}({a}.{col})"
        for col, fn in [
            ("device_id", "textsend"),
            ("metric", "textsend"),
            ("value", "float8send"),
            ("unit", "textsend"),
            ("event_time", "timestamptz_send"),
        ]
    )
    cur.execute(
        f"""SELECT l.id,c.id FROM
      (SELECT {FIELDS} FROM public.shelly_measurements WHERE id>%s AND id<=%s) l
      FULL JOIN (SELECT {FIELDS} FROM shelly_compact.readings WHERE id>%s AND id<=%s) c USING(id)
      WHERE l.id IS NULL OR c.id IS NULL OR ROW({bits("l")}) IS DISTINCT FROM ROW({bits("c")}) LIMIT 1""",
        (lo, hi, lo, hi),
    )
    if cur.fetchone() is not None:
        raise ValueError("Conflicting measurements in copied ID range")


def batch(conn, direction, size=50000):
    if not 1 <= size <= 100000:
        raise ValueError("Batch size must be 1..100000")
    with conn, conn.cursor() as cur:
        cur.execute(
            "SET LOCAL lock_timeout='1s'; SET LOCAL statement_timeout='60s'; SET LOCAL max_parallel_workers_per_gather=0"
        )
        cur.execute(
            "SELECT high_water,last_id FROM shelly_compact.copy_progress WHERE direction=%s FOR UPDATE NOWAIT",
            (direction,),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError("Capture a drained-writer checkpoint first")
        high, lo = row
        if lo >= high:
            return None
        source = SOURCES[direction]
        cur.execute(
            f"SELECT max(id),count(*) FROM (SELECT id FROM {source} WHERE id>%s AND id<=%s ORDER BY id LIMIT %s) b",
            (lo, high, size),
        )
        hi, n = cur.fetchone()
        if hi is None:
            hi = high
        if n and direction == "forward":
            cur.execute(
                """INSERT INTO shelly_compact.series(device_id,metric,unit)
                SELECT DISTINCT device_id,metric,unit FROM public.shelly_measurements WHERE id>%s AND id<=%s
                ORDER BY device_id,metric,unit ON CONFLICT(device_id,metric,unit) DO NOTHING""",
                (lo, hi),
            )
            cur.execute(
                """INSERT INTO shelly_compact.measurements(id,series_id,value,event_time)
                SELECT m.id,s.series_id,m.value,m.event_time
                FROM public.shelly_measurements m JOIN shelly_compact.series s
                ON s.device_id=m.device_id AND s.metric=m.metric AND s.unit IS NOT DISTINCT FROM m.unit
                WHERE m.id>%s AND m.id<=%s ORDER BY m.id ON CONFLICT(id) DO NOTHING""",
                (lo, hi),
            )
        elif n:
            cur.execute(
                f"""INSERT INTO public.shelly_measurements({FIELDS})
                SELECT {FIELDS} FROM shelly_compact.readings WHERE id>%s AND id<=%s ORDER BY id
                ON CONFLICT(id) DO NOTHING""",
                (lo, hi),
            )
        inserted = cur.rowcount if n else 0
        # Never silently accept an existing ID with different raw measurements.
        if n and inserted != n:
            verify_range(cur, lo, hi)
        cur.execute(
            """UPDATE shelly_compact.copy_progress SET last_id=%s,
            copied_rows=copied_rows+%s,updated_at=now() WHERE direction=%s""",
            (hi, n, direction),
        )
        return {
            "direction": direction,
            "last_id": hi,
            "high_water": high,
            "rows": n,
            "inserted": inserted,
        }


def build_index(conn):
    conn.rollback()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SET statement_timeout='20min'; SET lock_timeout='5s'; SET max_parallel_maintenance_workers=0"
            )
            cur.execute(
                "SELECT pg_get_indexdef(i.indexrelid),i.indisvalid,i.indisready FROM pg_index i WHERE i.indexrelid=to_regclass('shelly_compact.measurements_series_time_covering')"
            )
            existing = cur.fetchone()
            expected = "CREATE INDEX measurements_series_time_covering ON shelly_compact.measurements USING btree (series_id, event_time DESC) INCLUDE (value)"
            if existing:
                if existing != (expected, True, True):
                    raise ValueError(
                        "Unexpected or incomplete covering index; inspect before retrying"
                    )
                return {"status": "PASS", "phase": "index", "already_valid": True}
            cur.execute(
                "CREATE INDEX CONCURRENTLY measurements_series_time_covering ON shelly_compact.measurements (series_id,event_time DESC) INCLUDE(value)"
            )
            cur.execute(
                "ANALYZE shelly_compact.series; ANALYZE shelly_compact.measurements"
            )
            return {"status": "PASS", "phase": "index", "already_valid": False}
    finally:
        with conn.cursor() as cur:
            cur.execute(
                "RESET statement_timeout; RESET lock_timeout; RESET max_parallel_maintenance_workers"
            )
        conn.autocommit = False


class Digest:
    def __init__(self):
        self.sha = hashlib.sha256()
        self.size = 0

    def write(self, data):
        self.sha.update(data)
        self.size += len(data)


def verify(conn):
    conn.rollback()
    conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
    started = time.monotonic()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "SET LOCAL statement_timeout='20min'; SET LOCAL max_parallel_workers_per_gather=0"
            )
            cur.execute(
                "SELECT indisvalid AND indisready FROM pg_index WHERE indexrelid=to_regclass('shelly_compact.measurements_series_time_covering')"
            )
            if cur.fetchone() != (True,):
                raise ValueError(
                    "Build and validate the covering index before final verification"
                )
            results = {}
            for name, source in SOURCES.items():
                digest = Digest()
                cur.copy_expert(
                    f"COPY (SELECT {FIELDS} FROM {source} ORDER BY id) TO STDOUT (FORMAT binary)",
                    digest,
                )
                cur.execute(f"SELECT count(*),min(id),max(id) FROM {source}")
                count, first, last = cur.fetchone()
                results[name] = {
                    "sha256": digest.sha.hexdigest(),
                    "bytes": digest.size,
                    "rows": count,
                    "min_id": first,
                    "max_id": last,
                }
            if results["forward"] != results["reverse"]:
                raise ValueError("Full snapshot differs; do not switch readers/writers")
            cur.execute(
                "SELECT count(*) FROM pg_index WHERE indrelid IN ('shelly_compact.measurements'::regclass,'shelly_compact.series'::regclass) AND (NOT indisvalid OR NOT indisready)"
            )
            if cur.fetchone()[0]:
                raise ValueError("Invalid compact indexes")
            cur.execute(
                "SELECT count(*) FROM pg_constraint WHERE conrelid='shelly_compact.measurements'::regclass AND NOT convalidated"
            )
            if cur.fetchone()[0]:
                raise ValueError("Unvalidated compact constraints")
        return {
            "status": "PASS",
            "method": "full ordered binary COPY in one repeatable-read snapshot",
            "results": results,
            "seconds": round(time.monotonic() - started, 2),
        }
    finally:
        conn.set_session(isolation_level="READ COMMITTED", readonly=False)


def check_headroom(
    conn, path, minimum_free_bytes=20 * 1024**3, maximum_wal_bytes=4 * 1024**3
):
    """Path must be a read-only mount of the same filesystem as PostgreSQL data."""
    stats = os.statvfs(path)
    free = stats.f_bavail * stats.f_frsize
    if free < minimum_free_bytes:
        raise RuntimeError("Copy paused: database filesystem below free-space reserve")
    with conn, conn.cursor() as cur:
        cur.execute("SELECT coalesce(sum(size),0) FROM pg_ls_waldir()")
        wal = int(cur.fetchone()[0])
    if wal > maximum_wal_bytes:
        raise RuntimeError("Copy paused: retained WAL exceeds the migration budget")
    return {"free_bytes": free, "retained_wal_bytes": wal}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("phase", choices=["prepare", "capture", "copy", "index", "verify"])
    p.add_argument("--direction", choices=list(SOURCES), default="forward")
    p.add_argument("--writers-paused", action="store_true")
    p.add_argument("--batch-size", type=int, default=50000)
    p.add_argument("--pause", type=float, default=0.2)
    p.add_argument("--production", action="store_true")
    p.add_argument("--expected-system-id")
    p.add_argument("--receipt", type=Path)
    p.add_argument("--space-check-path", type=Path)
    p.add_argument("--minimum-free-gib", type=int, default=20)
    p.add_argument("--maximum-wal-gib", type=int, default=4)
    a = p.parse_args()
    dsn = os.environ["SHELLY_MIGRATION_DSN"]
    parts = parse_dsn(dsn)
    if not a.production and parts.get("host") not in {"127.0.0.1", "localhost"}:
        p.error(
            "Non-local database requires explicit --production and expected cluster ID"
        )
    if a.production and not a.expected_system_id:
        p.error("--expected-system-id required for production")
    if a.phase == "capture" and not a.writers_paused:
        p.error("Pause/drain all writers before checkpoint capture")
    if a.pause < 0:
        p.error("Pause cannot be negative")
    if a.minimum_free_gib < 20 or not 1 <= a.maximum_wal_gib <= 4:
        p.error(
            "Keep at least 20 GiB free and a maximum retained WAL budget of 1..4 GiB"
        )
    if a.production and a.phase in {"copy", "index"} and not a.space_check_path:
        p.error(
            "Production copy/index requires --space-check-path on the verified database filesystem"
        )
    with closing(
        psycopg2.connect(dsn, application_name="shelly_compact_migration")
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT system_identifier::text FROM pg_control_system()")
            identity = cur.fetchone()[0]
            if a.expected_system_id and identity != a.expected_system_id:
                raise ValueError("Wrong PostgreSQL cluster")
        conn.commit()
        if a.phase == "prepare":
            prepare(conn)
            result = {"status": "PASS", "phase": "prepare"}
        elif a.phase == "capture":
            result = {"status": "PASS", "high_water": capture(conn, a.direction)}
        elif a.phase == "copy":
            last_print = 0
            total = 0
            while True:
                if a.space_check_path:
                    check_headroom(
                        conn,
                        a.space_check_path,
                        a.minimum_free_gib * 1024**3,
                        a.maximum_wal_gib * 1024**3,
                    )
                progress = batch(conn, a.direction, a.batch_size)
                if progress is None:
                    break
                total += progress["rows"]
                if time.monotonic() - last_print > 20:
                    print(json.dumps(progress), flush=True)
                    last_print = time.monotonic()
                time.sleep(a.pause)
            result = {
                "status": "PASS",
                "phase": "copy",
                "direction": a.direction,
                "rows_this_run": total,
            }
        elif a.phase == "index":
            with conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT last_id>=high_water FROM shelly_compact.copy_progress WHERE direction='forward'"
                )
                if cur.fetchone() != (True,):
                    raise ValueError(
                        "Complete forward copy before building the covering index"
                    )
            if a.space_check_path:
                check_headroom(
                    conn,
                    a.space_check_path,
                    a.minimum_free_gib * 1024**3,
                    a.maximum_wal_gib * 1024**3,
                )
            result = build_index(conn)
        else:
            result = verify(conn)
    if a.receipt:
        a.receipt.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
