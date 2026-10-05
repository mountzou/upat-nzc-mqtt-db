"""Compact measurement inserts in the caller's transaction."""


def insert_measurement(conn, device_id, metric, value, unit=None, event_time=None):
    if conn.autocommit:
        raise ValueError("Measurement writes require a caller-owned transaction")
    with conn.cursor() as cur:
        cur.execute("SELECT nextval('public.shelly_measurements_id_seq'::regclass)")
        measurement_id = cur.fetchone()[0]
        cur.execute(
            """INSERT INTO shelly_compact.measurements(id,series_id,value,event_time)
            VALUES (%s,shelly_compact.resolve_series(%s,%s,%s),%s,%s)""",
            (measurement_id, device_id, metric, unit, value, event_time),
        )
    return measurement_id
