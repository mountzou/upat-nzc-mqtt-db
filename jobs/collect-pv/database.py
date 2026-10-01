"""Transactional PostgreSQL persistence for validated live PV batches."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any


TYPED_DEVICE_FIELDS = (
    "active_power_kw",
    "reactive_power_kvar",
    "mppt_power_kw",
    "power_factor",
    "efficiency_percent",
    "temperature_c",
    "grid_frequency_hz",
    "inverter_state",
    "day_energy_kwh",
    "total_energy_kwh",
)


class PersistenceError(RuntimeError):
    """The batch could not be committed to PostgreSQL."""


def db_connect():
    """Open one PostgreSQL connection from the standard service variables."""
    import psycopg2

    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.getenv("POSTGRES_INTERNAL_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=int(os.getenv("POSTGRES_CONNECT_TIMEOUT_SECONDS", "10")),
        application_name="collect-pv",
    )


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _observed_bounds(rows: list[dict[str, Any]]) -> tuple[datetime, datetime]:
    timestamps = [
        datetime.fromisoformat(row["observed_at"])
        for row in rows
    ]
    return min(timestamps), max(timestamps)


def _upsert_was_insert(cursor) -> bool:
    returned = cursor.fetchone()
    if returned is None:
        raise PersistenceError("PostgreSQL upsert returned no result")
    return bool(returned[0])


def _persist_transaction(cursor, batch: dict[str, Any]) -> dict[str, Any]:
    plant = batch["plant"]
    devices = batch["devices"]
    device_readings = batch["device_readings"]
    plant_readings = batch["plant_readings"]
    request = batch["request_window"]
    quality = batch["quality"]
    first_seen, last_seen = _observed_bounds(device_readings)

    cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s));", (plant["provider_plant_dn"],))
    cursor.execute(
        """
        INSERT INTO pv_plants (
            provider,
            provider_plant_dn,
            timezone,
            first_seen_at,
            last_seen_at
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (provider, provider_plant_dn) DO UPDATE
        SET timezone = EXCLUDED.timezone,
            is_active = TRUE,
            first_seen_at = LEAST(pv_plants.first_seen_at, EXCLUDED.first_seen_at),
            last_seen_at = GREATEST(pv_plants.last_seen_at, EXCLUDED.last_seen_at),
            updated_at = NOW()
        RETURNING id;
        """,
        (
            batch["source"],
            plant["provider_plant_dn"],
            plant["timezone"],
            first_seen,
            last_seen,
        ),
    )
    plant_id = int(cursor.fetchone()[0])

    readings_by_device: dict[str, list[dict[str, Any]]] = {}
    for row in device_readings:
        readings_by_device.setdefault(str(row["provider_device_id"]), []).append(row)

    database_device_ids: dict[str, int] = {}
    for device in devices:
        provider_id = str(device["provider_device_id"])
        observed = readings_by_device.get(provider_id, [])
        device_first = device_last = None
        if observed:
            device_first, device_last = _observed_bounds(observed)
        cursor.execute(
            """
            INSERT INTO pv_devices (
                plant_id,
                provider_device_id,
                provider_device_dn,
                device_type_code,
                device_role,
                name,
                model,
                software_version,
                first_seen_at,
                last_seen_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (plant_id, provider_device_id) DO UPDATE
            SET provider_device_dn = COALESCE(
                    EXCLUDED.provider_device_dn,
                    pv_devices.provider_device_dn
                ),
                device_type_code = EXCLUDED.device_type_code,
                device_role = EXCLUDED.device_role,
                name = COALESCE(EXCLUDED.name, pv_devices.name),
                model = COALESCE(EXCLUDED.model, pv_devices.model),
                software_version = COALESCE(
                    EXCLUDED.software_version,
                    pv_devices.software_version
                ),
                is_active = TRUE,
                first_seen_at = LEAST(
                    pv_devices.first_seen_at,
                    EXCLUDED.first_seen_at
                ),
                last_seen_at = GREATEST(
                    pv_devices.last_seen_at,
                    EXCLUDED.last_seen_at
                ),
                updated_at = NOW()
            RETURNING id;
            """,
            (
                plant_id,
                provider_id,
                device.get("provider_device_dn"),
                device["device_type_code"],
                device["device_role"],
                device.get("name"),
                device.get("model"),
                device.get("software_version"),
                device_first,
                device_last,
            ),
        )
        database_device_ids[provider_id] = int(cursor.fetchone()[0])

    request_start = datetime.fromtimestamp(int(request["start_ms"]) / 1000, timezone.utc)
    request_end = datetime.fromtimestamp(int(request["end_ms"]) / 1000, timezone.utc)
    cursor.execute(
        """
        INSERT INTO pv_ingestion_runs (
            plant_id,
            run_key,
            status,
            request_start_date,
            request_end_date,
            request_start_at,
            request_end_at,
            request_timezone,
            lookback_days,
            expected_device_count,
            returned_device_count,
            device_reading_count,
            plant_reading_count,
            call_summary
        )
        VALUES (
            %s, %s, 'running', %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, CAST(%s AS JSONB)
        )
        RETURNING id;
        """,
        (
            plant_id,
            batch["run_key"],
            request["start_date"],
            request["end_date"],
            request_start,
            request_end,
            request["timezone"],
            request["lookback_days"],
            len(devices),
            len(devices),
            len(device_readings),
            len(plant_readings),
            _json(batch.get("api_calls") or []),
        ),
    )
    run_id = int(cursor.fetchone()[0])

    inserted_count = 0
    updated_count = 0
    for row in device_readings:
        cursor.execute(
            """
            INSERT INTO pv_device_readings_5m (
                device_id,
                observed_at,
                local_date,
                source_run_id,
                provider_collect_time_ms,
                active_power_kw,
                reactive_power_kvar,
                mppt_power_kw,
                power_factor,
                efficiency_percent,
                temperature_c,
                grid_frequency_hz,
                inverter_state,
                day_energy_kwh,
                total_energy_kwh,
                extra_kpis,
                quality_flags
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, CAST(%s AS JSONB), %s
            )
            ON CONFLICT (device_id, observed_at) DO UPDATE
            SET local_date = EXCLUDED.local_date,
                source_run_id = EXCLUDED.source_run_id,
                provider_collect_time_ms = EXCLUDED.provider_collect_time_ms,
                active_power_kw = EXCLUDED.active_power_kw,
                reactive_power_kvar = EXCLUDED.reactive_power_kvar,
                mppt_power_kw = EXCLUDED.mppt_power_kw,
                power_factor = EXCLUDED.power_factor,
                efficiency_percent = EXCLUDED.efficiency_percent,
                temperature_c = EXCLUDED.temperature_c,
                grid_frequency_hz = EXCLUDED.grid_frequency_hz,
                inverter_state = EXCLUDED.inverter_state,
                day_energy_kwh = EXCLUDED.day_energy_kwh,
                total_energy_kwh = EXCLUDED.total_energy_kwh,
                extra_kpis = EXCLUDED.extra_kpis,
                quality_flags = EXCLUDED.quality_flags,
                updated_at = NOW()
            RETURNING (xmax = 0);
            """,
            (
                database_device_ids[str(row["provider_device_id"])],
                row["observed_at"],
                row["local_date"],
                run_id,
                row["provider_collect_time_ms"],
                *(row.get(field) for field in TYPED_DEVICE_FIELDS),
                _json(row.get("extra_kpis") or {}),
                row.get("quality_flags") or [],
            ),
        )
        if _upsert_was_insert(cursor):
            inserted_count += 1
        else:
            updated_count += 1

    source_reference = _json(
        {
            "derivation": "sum_inverter_readings",
            "schema_version": batch["schema_version"],
        }
    )
    for row in plant_readings:
        cursor.execute(
            """
            INSERT INTO pv_plant_readings_5m (
                plant_id,
                observed_at,
                local_date,
                source_run_id,
                provider_collect_time_ms,
                aggregation_version,
                active_power_kw,
                reactive_power_kvar,
                mppt_power_kw,
                power_factor,
                reporting_device_count,
                expected_device_count,
                reporting_device_ids,
                missing_device_ids,
                quality_status,
                quality_flags,
                source_reference
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, CAST(%s AS JSONB)
            )
            ON CONFLICT (plant_id, observed_at) DO UPDATE
            SET local_date = EXCLUDED.local_date,
                source_run_id = EXCLUDED.source_run_id,
                provider_collect_time_ms = EXCLUDED.provider_collect_time_ms,
                aggregation_version = EXCLUDED.aggregation_version,
                active_power_kw = EXCLUDED.active_power_kw,
                reactive_power_kvar = EXCLUDED.reactive_power_kvar,
                mppt_power_kw = EXCLUDED.mppt_power_kw,
                power_factor = EXCLUDED.power_factor,
                reporting_device_count = EXCLUDED.reporting_device_count,
                expected_device_count = EXCLUDED.expected_device_count,
                reporting_device_ids = EXCLUDED.reporting_device_ids,
                missing_device_ids = EXCLUDED.missing_device_ids,
                quality_status = EXCLUDED.quality_status,
                quality_flags = EXCLUDED.quality_flags,
                source_reference = EXCLUDED.source_reference,
                updated_at = NOW()
            RETURNING (xmax = 0);
            """,
            (
                plant_id,
                row["observed_at"],
                row["local_date"],
                run_id,
                row.get("provider_collect_time_ms"),
                batch["schema_version"],
                row.get("active_power_kw"),
                row.get("reactive_power_kvar"),
                row.get("mppt_power_kw"),
                row.get("power_factor"),
                row.get("reporting_device_count"),
                row.get("expected_device_count"),
                row.get("reporting_device_ids") or [],
                row.get("missing_device_ids") or [],
                row["quality_status"],
                row.get("quality_flags") or [],
                source_reference,
            ),
        )
        if _upsert_was_insert(cursor):
            inserted_count += 1
        else:
            updated_count += 1

    run_status = "success" if quality["status"] == "complete" else "partial"
    cursor.execute(
        """
        UPDATE pv_ingestion_runs
        SET status = %s,
            inserted_count = %s,
            updated_count = %s,
            rejected_count = 0,
            completed_at = NOW(),
            updated_at = NOW()
        WHERE id = %s;
        """,
        (run_status, inserted_count, updated_count, run_id),
    )
    if cursor.rowcount != 1:
        raise PersistenceError("ingestion run finalization did not update one row")

    collected_at = datetime.fromisoformat(batch["collected_at"])
    source_key = f"{batch['source']}:{plant['provider_plant_dn']}"
    cursor.execute(
        """
        INSERT INTO pv_source_state (
            source_key,
            provider,
            circuit_state,
            consecutive_failures,
            last_login_success_at,
            last_successful_ingestion_at
        )
        VALUES (%s, %s, 'closed', 0, %s, %s)
        ON CONFLICT (source_key) DO UPDATE
        SET circuit_state = 'closed',
            blocked_until = NULL,
            consecutive_failures = 0,
            last_fail_code = NULL,
            sanitized_last_error = NULL,
            last_login_success_at = EXCLUDED.last_login_success_at,
            last_successful_ingestion_at = EXCLUDED.last_successful_ingestion_at,
            updated_at = NOW();
        """,
        (source_key, batch["source"], collected_at, collected_at),
    )

    return {
        "status": "success",
        "adapter": "postgres-v1",
        "run_id": run_id,
        "rows_written": inserted_count + updated_count,
        "inserted_count": inserted_count,
        "updated_count": updated_count,
        "device_reading_count": len(device_readings),
        "plant_reading_count": len(plant_readings),
    }


def persist_batch(
    batch: dict[str, Any],
    *,
    connect: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    """Persist one live batch atomically and return a non-sensitive summary."""
    connector = connect or db_connect
    connection = None
    try:
        connection = connector()
        with connection:
            with connection.cursor() as cursor:
                return _persist_transaction(cursor, batch)
    except PersistenceError:
        raise
    except Exception as exc:
        detail = " ".join(str(exc).split())[:500]
        raise PersistenceError(
            f"PostgreSQL transaction rolled back: {type(exc).__name__}: {detail}"
        ) from exc
    finally:
        if connection is not None:
            connection.close()
