import os
from datetime import datetime, timedelta, timezone, time
from zoneinfo import ZoneInfo

import psycopg2

LOCAL_TZ = ZoneInfo("Europe/Athens")

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_INTERNAL_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Create and return a new PostgreSQL connection.
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

# Check whether the CRON execution time falls within working hours (8:00-14:00) on a working day (Monday-Friday).
def is_working_period(start_time_utc):
    start_local = start_time_utc.astimezone(LOCAL_TZ)

    is_working_day  = 1 if start_local.weekday() < 5 else 0
    is_working_hour = 1 if is_working_day and 8 <= start_local.hour < 14 else 0

    return is_working_day, is_working_hour

# Get the list of Shelly device IDs and split them into plugs and pro3em devices.
def get_shelly_device_ids(rows):
    plug_ids = []
    pro3em_ids = []

    for row in rows:
        device_id = row[0]

        if device_id.startswith("shellyplug"):
            plug_ids.append(device_id)
        elif device_id.startswith("shellypro3em"):
            pro3em_ids.append(device_id)

    return plug_ids, pro3em_ids

def main():
    now = datetime.now(timezone.utc)
    end_time = now.replace(minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(hours=1)

    is_working_day, is_working_hour = is_working_period(start_time)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT device_id
                FROM shelly_devices;
            """)
            rows = cur.fetchall()

            plug_ids, pro3em_ids = get_shelly_device_ids(rows)

            common_window_info = {
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "is_working_day": is_working_day,
                "is_working_hour": is_working_hour,
            }

            # Process Shelly Plug S devices.
            for device_id in plug_ids:
                cur.execute(
                    """
                    SELECT value, event_time
                    FROM shelly_measurements
                    WHERE device_id = %s
                      AND metric = 'aenergy_total'
                      AND event_time >= %s
                      AND event_time < %s
                    ORDER BY event_time ASC
                    LIMIT 1;
                    """,
                    (device_id, start_time, end_time),
                )
                first_row = cur.fetchone()

                cur.execute(
                    """
                    SELECT value, event_time
                    FROM shelly_measurements
                    WHERE device_id = %s
                      AND metric = 'aenergy_total'
                      AND event_time >= %s
                      AND event_time < %s
                    ORDER BY event_time DESC
                    LIMIT 1;
                    """,
                    (device_id, start_time, end_time),
                )
                last_row = cur.fetchone()

                if not first_row or not last_row:
                    continue

                first_value, first_ts = first_row
                last_value, last_ts = last_row
                delta_wh = float(last_value) - float(first_value)

                if delta_wh < 0:
                    continue

                cur.execute(
                    """
                    INSERT INTO shelly_plug_hourly_energy (
                        device_id,
                        window_start,
                        window_end,
                        energy_wh,
                        is_working_day,
                        is_working_hour
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (device_id, window_start, window_end)
                    DO UPDATE SET
                        energy_wh = EXCLUDED.energy_wh,
                        is_working_day = EXCLUDED.is_working_day,
                        is_working_hour = EXCLUDED.is_working_hour,
                        created_at = NOW();
                    """,
                    (
                        device_id,
                        start_time,
                        end_time,
                        round(delta_wh, 3),
                        is_working_day,
                        is_working_hour,
                    ),
                )

            # Process Shelly 3EM Pro devices.
            for device_id in pro3em_ids:
                cur.execute(
                    """
                    SELECT event_time, metric, value
                    FROM shelly_measurements
                    WHERE device_id = %s
                      AND metric IN ('a_act_power', 'b_act_power', 'c_act_power')
                      AND event_time >= %s
                      AND event_time < %s
                    ORDER BY event_time ASC;
                    """,
                    (device_id, start_time, end_time),
                )
                rows = cur.fetchall()

                by_time = {}
                for event_time, metric, value in rows:
                    if event_time not in by_time:
                        by_time[event_time] = {}
                    by_time[event_time][metric] = float(value)

                timestamps = sorted(by_time.keys())

                if len(timestamps) < 2:
                    continue

                energy = {"a": 0.0, "b": 0.0, "c": 0.0}
                phase_map = {
                    "a": "a_act_power",
                    "b": "b_act_power",
                    "c": "c_act_power",
                }

                for i in range(1, len(timestamps)):
                    t0 = timestamps[i - 1]
                    t1 = timestamps[i]
                    dt_hours = (t1 - t0).total_seconds() / 3600.0

                    prev_vals = by_time[t0]
                    curr_vals = by_time[t1]

                    for phase, metric in phase_map.items():
                        if metric in prev_vals and metric in curr_vals:
                            p0 = prev_vals[metric]
                            p1 = curr_vals[metric]
                            energy[phase] += ((p0 + p1) / 2.0) * dt_hours

                total_energy = energy["a"] + energy["b"] + energy["c"]

                cur.execute(
                    """
                    INSERT INTO shelly_pro3em_hourly_energy (
                        device_id,
                        window_start,
                        window_end,
                        a_energy_wh,
                        b_energy_wh,
                        c_energy_wh,
                        total_energy_wh,
                        is_working_day,
                        is_working_hour
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (device_id, window_start, window_end)
                    DO UPDATE SET
                        a_energy_wh = EXCLUDED.a_energy_wh,
                        b_energy_wh = EXCLUDED.b_energy_wh,
                        c_energy_wh = EXCLUDED.c_energy_wh,
                        total_energy_wh = EXCLUDED.total_energy_wh,
                        is_working_day = EXCLUDED.is_working_day,
                        is_working_hour = EXCLUDED.is_working_hour,
                        created_at = NOW();
                    """,
                    (
                        device_id,
                        start_time,
                        end_time,
                        round(energy["a"], 3),
                        round(energy["b"], 3),
                        round(energy["c"], 3),
                        round(total_energy, 3),
                        is_working_day,
                        is_working_hour,
                    ),
                )


if __name__ == "__main__":
    main()