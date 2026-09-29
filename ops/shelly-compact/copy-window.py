#!/usr/bin/env python3
"""Bounded production copy windows using the already verified batch implementation."""

import argparse
import importlib.util
import json
import os
import time
from contextlib import closing
from pathlib import Path

import psycopg2


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--batch-size", type=int, default=10000)
    p.add_argument("--batches", type=int, default=40)
    p.add_argument("--pause", type=float, default=0.5)
    p.add_argument("--receipt", type=Path, required=True)
    a = p.parse_args()
    if not 1 <= a.batch_size <= 50000 or not 1 <= a.batches <= 2000 or a.pause < 0.2:
        p.error("Production limits: 1..50000 rows, 1..2000 batches, pause >=0.2s")
    spec = importlib.util.spec_from_file_location(
        "migration", Path(__file__).with_name("migrate.py")
    )
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    started = time.monotonic()
    rows = 0
    last = None
    state = "PAUSED"
    last_print = 0
    minimum_free = None
    max_wal = 0
    with closing(
        psycopg2.connect(
            os.environ["SHELLY_MIGRATION_DSN"],
            application_name="shelly_compact_migration",
        )
    ) as conn:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT system_identifier::text FROM pg_control_system()")
            if cur.fetchone() != ("7618955918777626661",):
                raise RuntimeError("Wrong production cluster")
        for _ in range(a.batches):
            capacity = m.check_headroom(conn, "/space-check")
            minimum_free = min(
                minimum_free or capacity["free_bytes"], capacity["free_bytes"]
            )
            max_wal = max(max_wal, capacity["retained_wal_bytes"])
            progress = m.batch(conn, "forward", a.batch_size)
            if progress is None:
                state = "COMPLETE"
                break
            rows += progress["rows"]
            last = progress
            if time.monotonic() - last_print > 20:
                print(json.dumps(progress | capacity), flush=True)
                last_print = time.monotonic()
            time.sleep(a.pause)
    result = {
        "status": "PASS",
        "state": state,
        "rows_this_window": rows,
        "last": last,
        "seconds": round(time.monotonic() - started, 2),
        "minimum_free_bytes": minimum_free,
        "maximum_retained_wal_bytes": max_wal,
    }
    a.receipt.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
