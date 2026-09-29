"""Reversible measurement storage; all writes use the caller's transaction."""

import os

WRITE_MODE = os.getenv("SHELLY_MEASUREMENTS_WRITE_MODE", "legacy")
if WRITE_MODE not in {"legacy", "dual", "compact"}:
    raise ValueError("Invalid SHELLY_MEASUREMENTS_WRITE_MODE")


def insert_measurement(
    conn, device_id, metric, value, unit=None, event_time=None, *, mode=None
):
    mode = WRITE_MODE if mode is None else mode
    if mode not in {"legacy", "dual", "compact"}:
        raise ValueError("Invalid measurement write mode")
    if conn.autocommit:
        raise ValueError("Measurement writes require a caller-owned transaction")
    with conn.cursor() as cur:
        if mode in {"legacy", "dual"}:
            cur.execute(
                """INSERT INTO public.shelly_measurements
                (device_id,metric,value,unit,event_time) VALUES (%s,%s,%s,%s,%s)
                RETURNING id""",
                (device_id, metric, value, unit, event_time),
            )
            measurement_id = cur.fetchone()[0]
        else:
            cur.execute("SELECT nextval('public.shelly_measurements_id_seq'::regclass)")
            measurement_id = cur.fetchone()[0]
        if mode in {"dual", "compact"}:
            cur.execute(
                """INSERT INTO shelly_compact.measurements(id,series_id,value,event_time)
                VALUES (%s,shelly_compact.resolve_series(%s,%s,%s),%s,%s)""",
                (measurement_id, device_id, metric, unit, value, event_time),
            )
    return measurement_id
