import copy
import json
import os
import unittest
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

from persistence import PersistenceContractError, PersistenceError, persist_batch
from pipeline import build_ingestion_batch, build_request_window


FIXTURE_PATH = Path(__file__).parent / "tests" / "fixtures" / "fusionsolar_sample.json"


def build_live_batch(plant_code: str) -> dict:
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    target_date = date.fromisoformat(fixture["target_date"])
    return build_ingestion_batch(
        plant_code=plant_code,
        request_window=build_request_window(target_date=target_date, lookback_days=1),
        device_list_payload=fixture["device_list"],
        history_by_device_type={
            int(key): value
            for key, value in fixture["history_by_device_type"].items()
        },
        source_kind="fusion_live",
        api_calls=fixture["api_calls"],
        collected_at=datetime(2026, 8, 25, tzinfo=timezone.utc),
    )


class PersistenceContractTests(unittest.TestCase):
    def test_fixture_batch_is_rejected_before_connecting(self):
        batch = build_live_batch("NE=READ-ONLY")
        batch["source_kind"] = "fixture"
        connected = False

        def forbidden_connect():
            nonlocal connected
            connected = True
            raise AssertionError("database connection must not be attempted")

        with self.assertRaisesRegex(PersistenceContractError, "fixtures"):
            persist_batch(
                batch,
                site_key="school_test",
                connect=forbidden_connect,
            )
        self.assertFalse(connected)

    def test_inconsistent_provider_timestamp_is_rejected_before_connecting(self):
        batch = build_live_batch("NE=INVALID-TIMESTAMP")
        batch["device_readings"][0]["provider_collect_time_ms"] += 1
        connected = False

        def forbidden_connect():
            nonlocal connected
            connected = True
            raise AssertionError("database connection must not be attempted")

        with self.assertRaisesRegex(PersistenceContractError, "does not match"):
            persist_batch(
                batch,
                site_key="school_test",
                connect=forbidden_connect,
            )
        self.assertFalse(connected)


@unittest.skipUnless(
    os.getenv("PV_INGESTOR_INTEGRATION_TEST") == "1",
    "requires an explicitly isolated PostgreSQL integration database",
)
class PersistenceIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg2

        database_name = os.getenv("POSTGRES_DB", "")
        if "test" not in database_name.lower():
            raise RuntimeError(
                "persistence integration tests require a database name containing 'test'"
            )
        cls.psycopg2 = psycopg2

    def connect(self):
        return self.psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=int(os.getenv("POSTGRES_INTERNAL_PORT", "5432")),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )

    def test_repeated_batch_is_idempotent_and_audited(self):
        suffix = uuid.uuid4().hex
        site_key = f"school-test-{suffix}"
        batch = build_live_batch(f"NE=IDEMPOTENT-{suffix}")

        first = persist_batch(
            batch,
            site_key=site_key,
            trigger_kind="manual",
            code_version="integration-test",
            connect=self.connect,
        )
        second = persist_batch(
            batch,
            site_key=site_key,
            trigger_kind="manual",
            code_version="integration-test",
            connect=self.connect,
        )

        self.assertEqual(10, first["inserted_count"])
        self.assertEqual(0, first["updated_count"])
        self.assertEqual(0, second["inserted_count"])
        self.assertEqual(10, second["updated_count"])
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM pv_ingestion_runs WHERE plant_id = p.id),
                        (SELECT COUNT(*)
                         FROM pv_device_readings_5m r
                         JOIN pv_devices d ON d.id = r.device_id
                         WHERE d.plant_id = p.id),
                        (SELECT COUNT(*) FROM pv_plant_readings_5m WHERE plant_id = p.id),
                        (SELECT extra_kpis->>'pv1_u'
                         FROM pv_device_readings_5m r
                         JOIN pv_devices d ON d.id = r.device_id
                         WHERE d.plant_id = p.id
                           AND d.provider_device_id = '101'
                         ORDER BY r.observed_at
                         LIMIT 1),
                        (SELECT source_kind
                         FROM pv_plant_readings_5m
                         WHERE plant_id = p.id
                         ORDER BY observed_at
                         LIMIT 1),
                        (SELECT COUNT(*)
                         FROM pv_source_state
                         WHERE source_key = 'huawei_fusionsolar:' || p.provider_plant_dn
                           AND circuit_state = 'closed'
                           AND consecutive_failures = 0)
                    FROM pv_plants p
                    WHERE p.site_key = %s;
                    """,
                    (site_key,),
                )
                self.assertEqual(
                    (2, 7, 3, "500.0", "fusion_live_device_derived", 1),
                    cursor.fetchone(),
                )
                cursor.execute(
                    """
                    SELECT
                        status,
                        request_start_date::TEXT,
                        request_end_date::TEXT,
                        JSONB_ARRAY_LENGTH(call_summary),
                        inserted_count,
                        updated_count
                    FROM pv_ingestion_runs
                    WHERE plant_id = (
                        SELECT id FROM pv_plants WHERE site_key = %s
                    )
                    ORDER BY id;
                    """,
                    (site_key,),
                )
                self.assertEqual(
                    [
                        ("partial", "2026-08-24", "2026-08-24", 4, 10, 0),
                        ("partial", "2026-08-24", "2026-08-24", 4, 0, 10),
                    ],
                    cursor.fetchall(),
                )

    def test_invalid_reading_rolls_back_the_entire_batch(self):
        suffix = uuid.uuid4().hex
        site_key = f"school-test-{suffix}"
        batch = copy.deepcopy(build_live_batch(f"NE=ROLLBACK-{suffix}"))
        batch["device_readings"][0]["day_energy_kwh"] = -1.0

        with self.assertRaisesRegex(PersistenceError, "rolled back"):
            persist_batch(
                batch,
                site_key=site_key,
                trigger_kind="manual",
                code_version="integration-test",
                connect=self.connect,
            )
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM pv_plants WHERE site_key = %s;",
                    (site_key,),
                )
                self.assertEqual(0, cursor.fetchone()[0])


if __name__ == "__main__":
    unittest.main()
