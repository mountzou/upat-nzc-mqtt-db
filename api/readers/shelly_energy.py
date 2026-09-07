"""Precomputed Shelly hourly energy reads shared by routes and school insights.

Keep query ordering, Wh values and native timestamps unchanged. HTTP callers
may use the existing string bounds; monitoring callers pass explicit instants.
"""
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException

import database
from monitoring.utils.timezone import as_utc
from schemas import parse_telemetry_bound


def normalize_device_ids(device_ids: list[str] | None):
    if not device_ids:
        return None
    return sorted({d.strip() for d in device_ids if d and d.strip()}) or None


def resolve_energy_time_bounds(
    start: str | datetime | None, end: str | datetime | None
):
    def parse_bound(value, name):
        return as_utc(value) if isinstance(value, datetime) else parse_telemetry_bound(value, name)

    now = datetime.now(timezone.utc)
    default_end = now.replace(minute=0, second=0, microsecond=0)
    default_start = default_end - timedelta(hours=24)

    start_time = parse_bound(start, "start") if start is not None else default_start
    end_time = parse_bound(end, "end") if end is not None else default_end

    if start_time > end_time:
        raise HTTPException(
            status_code=400,
            detail="start must be earlier than or equal to end",
        )

    return start_time, end_time


def split_shelly_device_ids(device_ids: list[str]):
    plug_ids    = [d for d in device_ids if d.startswith("shellyplug")]
    pro3em_ids  = [d for d in device_ids if d.startswith("shellypro3em")]
    unknown_ids = [d for d in device_ids if d not in plug_ids and d not in pro3em_ids]

    if unknown_ids:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown Shelly device type for device_ids={unknown_ids}",
        )

    return plug_ids, pro3em_ids


def fetch_shelly_hourly_energy_rows(
    device_id, start: str | datetime | None, end: str | datetime | None,
    working_only: bool, *, connection_factory=None,
):
    """Hourly device query retained for insights and device telemetry."""
    def energy_value(value):
        return round(float(value or 0.0), 3)

    device_ids = normalize_device_ids(device_id)

    if not device_ids:
        raise HTTPException(
            status_code=400,
            detail="At least one device_id must be provided",
        )

    start_time, end_time = resolve_energy_time_bounds(start, end)
    plug_ids, pro3em_ids = split_shelly_device_ids(device_ids)

    items = []

    connect = connection_factory if connection_factory is not None else database.get_connection
    with connect() as conn:
        with conn.cursor() as cur:
            if plug_ids:
                cur.execute(
                    """
                    SELECT
                        device_id,
                        window_start,
                        window_end,
                        energy_wh,
                        is_working_day,
                        is_working_hour,
                        created_at
                    FROM shelly_plug_hourly_energy
                    WHERE device_id = ANY(%s)
                      AND window_start >= %s
                      AND window_end <= %s
                      AND (%s = FALSE OR (is_working_day = 1 AND is_working_hour = 1))
                    ORDER BY window_start DESC, device_id ASC;
                    """,
                    (plug_ids, start_time, end_time, working_only),
                )
                rows = cur.fetchall()

                for row in rows:
                    items.append({
                        "device_id": row["device_id"],
                        "device_type": "plug",
                        "window_start": row["window_start"],
                        "window_end": row["window_end"],
                        "is_working_day": row["is_working_day"],
                        "is_working_hour": row["is_working_hour"],
                        "energy_wh": {
                            "total": energy_value(row["energy_wh"]),
                        },
                        "created_at": row["created_at"],
                    })

            if pro3em_ids:
                cur.execute(
                    """
                    SELECT
                        device_id,
                        window_start,
                        window_end,
                        a_energy_wh,
                        b_energy_wh,
                        c_energy_wh,
                        total_energy_wh,
                        is_working_day,
                        is_working_hour,
                        created_at
                    FROM shelly_pro3em_hourly_energy
                    WHERE device_id = ANY(%s)
                      AND window_start >= %s
                      AND window_end <= %s
                      AND (%s = FALSE OR (is_working_day = 1 AND is_working_hour = 1))
                    ORDER BY window_start DESC, device_id ASC;
                    """,
                    (pro3em_ids, start_time, end_time, working_only),
                )
                rows = cur.fetchall()

                for row in rows:
                    items.append({
                        "device_id": row["device_id"],
                        "device_type": "pro3em",
                        "window_start": row["window_start"],
                        "window_end": row["window_end"],
                        "is_working_day": row["is_working_day"],
                        "is_working_hour": row["is_working_hour"],
                        "energy_wh": {
                            "a": energy_value(row["a_energy_wh"]),
                            "b": energy_value(row["b_energy_wh"]),
                            "c": energy_value(row["c_energy_wh"]),
                            "total": energy_value(row["total_energy_wh"]),
                        },
                        "created_at": row["created_at"],
                    })

    return {
        "device_ids": device_ids,
        "start": start_time,
        "end": end_time,
        "working_only": working_only,
        "count": len(items),
        "items": items,
    }
