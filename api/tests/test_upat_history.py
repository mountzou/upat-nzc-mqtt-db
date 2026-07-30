import os
import unittest
import uuid
from datetime import datetime, timedelta
from unittest.mock import patch

import main
from main import fetch_upat_rollup_history, resolve_upat_rollup_table
from psycopg2 import sql
from schemas import HistoryQueryParams


class _FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.query = None
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, params):
        self.query = query
        self.params = params

    def fetchall(self):
        return self.rows


class _FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def cursor(self):
        return self._cursor


class UpatHistoryTests(unittest.TestCase):
    def test_rollup_table_selection(self):
        cases = [
            (
                HistoryQueryParams(
                    aggregate="avg",
                    bucket_unit="minute",
                    bucket_size=1,
                ),
                None,
            ),
            (
                HistoryQueryParams(
                    aggregate="avg",
                    bucket_unit="minute",
                    bucket_size=5,
                ),
                "upat_measurements_5min",
            ),
            (
                HistoryQueryParams(
                    aggregate="avg",
                    bucket_unit="minute",
                    bucket_size=15,
                ),
                "upat_measurements_5min",
            ),
            (
                HistoryQueryParams(
                    aggregate="avg",
                    bucket_unit="minute",
                    bucket_size=7,
                ),
                None,
            ),
            (
                HistoryQueryParams(
                    aggregate="avg",
                    bucket_unit="minute",
                    bucket_size=60,
                ),
                "upat_measurements_hourly",
            ),
            (
                HistoryQueryParams(
                    aggregate="avg",
                    bucket_unit="hour",
                    bucket_size=1,
                ),
                "upat_measurements_hourly",
            ),
            (
                HistoryQueryParams(
                    aggregate="avg",
                    bucket_unit="day",
                    bucket_size=1,
                ),
                "upat_measurements_hourly",
            ),
        ]

        for params, expected in cases:
            with self.subTest(params=params):
                self.assertEqual(resolve_upat_rollup_table(params), expected)

    @patch("main.get_connection")
    def test_rollup_history_combines_raw_and_rollup_sources(self, mock_get_connection):
        cursor = _FakeCursor(
            [
                {
                    "device_id": "portable-test",
                    "metric": "temperature",
                    "value": 45.0,
                    "unit": "C",
                    "event_time": datetime(2026, 7, 29, 10, 0),
                }
            ]
        )
        mock_get_connection.return_value = _FakeConnection(cursor)
        params = HistoryQueryParams(
            metric=["temperature"],
            start="2026-07-29T10:00",
            end="2026-07-29T10:14",
            aggregate="avg",
            bucket_unit="minute",
            bucket_size=15,
            limit=96,
        )

        response = fetch_upat_rollup_history(
            "upat_measurements_5min",
            "portable-test",
            params,
        )

        self.assertIn("WITH rollup_state AS", cursor.query)
        self.assertIn("FROM upat_rollup_state", cursor.query)
        self.assertIn("FROM upat_measurements AS measurement", cursor.query)
        self.assertIn("FROM upat_measurements_5min AS rollup", cursor.query)
        self.assertIn(
            "measurement.id > state.last_measurement_id",
            cursor.query,
        )
        self.assertNotIn("NOT EXISTS", cursor.query)
        self.assertIn("UNION ALL", cursor.query)
        self.assertEqual(cursor.params[0], "5 minutes")
        self.assertEqual(cursor.params[-1], "15 minutes")
        self.assertEqual(
            response["items"][0]["measurements"]["temperature"]["value"],
            45.0,
        )

    @patch("main.get_connection")
    def test_hourly_rollup_uses_raw_hour_source(self, mock_get_connection):
        cursor = _FakeCursor([])
        mock_get_connection.return_value = _FakeConnection(cursor)
        params = HistoryQueryParams(
            metric=["co2"],
            start="2026-07-29T09:00",
            end="2026-07-29T10:59",
            aggregate="avg",
            bucket_unit="hour",
            bucket_size=1,
        )

        fetch_upat_rollup_history(
            "upat_measurements_hourly",
            "portable-test",
            params,
        )

        self.assertEqual(cursor.params[0], "1 hour")
        self.assertEqual(cursor.params[-1], "1 hour")
        self.assertIn("FROM upat_measurements_hourly AS rollup", cursor.query)


@unittest.skipUnless(
    os.getenv("UPAT_RUN_DB_TESTS") == "1",
    "set UPAT_RUN_DB_TESTS=1 against a disposable PostgreSQL database",
)
class UpatHistoryDatabaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema_name = f"upat_history_test_{uuid.uuid4().hex}"
        cls.original_get_connection = main.get_connection

        with cls.original_get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("CREATE SCHEMA {}").format(
                        sql.Identifier(cls.schema_name)
                    )
                )

        def get_test_connection():
            conn = cls.original_get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SET search_path TO {}").format(
                        sql.Identifier(cls.schema_name)
                    )
                )
            return conn

        main.get_connection = get_test_connection

        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE upat_measurements (
                        id BIGINT PRIMARY KEY,
                        device_id TEXT NOT NULL,
                        metric TEXT NOT NULL,
                        value DOUBLE PRECISION,
                        unit TEXT,
                        event_time TIMESTAMP NOT NULL
                    );

                    CREATE TABLE upat_measurements_5min (
                        device_id TEXT NOT NULL,
                        metric TEXT NOT NULL,
                        unit TEXT,
                        bucket_start TIMESTAMP NOT NULL,
                        value_avg DOUBLE PRECISION,
                        value_min DOUBLE PRECISION,
                        value_max DOUBLE PRECISION,
                        sample_count INTEGER NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (device_id, metric, bucket_start)
                    );

                    CREATE TABLE upat_measurements_hourly (
                        device_id TEXT NOT NULL,
                        metric TEXT NOT NULL,
                        unit TEXT,
                        bucket_start TIMESTAMP NOT NULL,
                        value_avg DOUBLE PRECISION,
                        value_min DOUBLE PRECISION,
                        value_max DOUBLE PRECISION,
                        sample_count INTEGER NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (device_id, metric, bucket_start)
                    );

                    CREATE TABLE upat_rollup_state (
                        pipeline_name TEXT PRIMARY KEY,
                        last_measurement_id BIGINT NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                """)

    @classmethod
    def tearDownClass(cls):
        main.get_connection = cls.original_get_connection

        with cls.original_get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(
                        sql.Identifier(cls.schema_name)
                    )
                )

    def setUp(self):
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    TRUNCATE
                        upat_measurements,
                        upat_measurements_5min,
                        upat_measurements_hourly,
                        upat_rollup_state;

                    INSERT INTO upat_rollup_state (
                        pipeline_name,
                        last_measurement_id
                    )
                    VALUES ('upat', 100);
                """)

    def test_partial_retained_raw_bucket_does_not_replace_rollup(self):
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO upat_measurements_5min (
                        device_id,
                        metric,
                        unit,
                        bucket_start,
                        value_avg,
                        value_min,
                        value_max,
                        sample_count
                    )
                    VALUES (
                        'portable-test',
                        'temperature',
                        'C',
                        TIMESTAMP '2026-07-01 10:00:00',
                        20,
                        10,
                        30,
                        3
                    );

                    INSERT INTO upat_measurements (
                        id,
                        device_id,
                        metric,
                        value,
                        unit,
                        event_time
                    )
                    VALUES
                        (
                            90,
                            'portable-test',
                            'temperature',
                            30,
                            'C',
                            TIMESTAMP '2026-07-01 10:04:00'
                        ),
                        (
                            101,
                            'portable-test',
                            'temperature',
                            50,
                            'C',
                            TIMESTAMP '2026-07-01 10:04:30'
                        );
                """)

        params = HistoryQueryParams(
            metric=["temperature"],
            start="2026-07-01T10:00",
            end="2026-07-01T10:14",
            aggregate="avg",
            bucket_unit="minute",
            bucket_size=15,
        )

        response = fetch_upat_rollup_history(
            "upat_measurements_5min",
            "portable-test",
            params,
        )

        self.assertEqual(response["count"], 1)
        self.assertEqual(
            response["items"][0]["measurements"]["temperature"]["value"],
            27.5,
        )

    def test_twelve_five_minute_rollups_form_four_fifteen_minute_buckets(self):
        first_bucket = datetime(2026, 7, 1, 12, 0)
        rows = [
            (
                "portable-test",
                "temperature",
                "C",
                first_bucket + timedelta(minutes=5 * index),
                float(index),
                float(index),
                float(index),
                1,
            )
            for index in range(12)
        ]

        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO upat_measurements_5min (
                        device_id,
                        metric,
                        unit,
                        bucket_start,
                        value_avg,
                        value_min,
                        value_max,
                        sample_count
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    rows,
                )

        params = HistoryQueryParams(
            metric=["temperature"],
            start="2026-07-01T12:00",
            end="2026-07-01T12:59",
            aggregate="avg",
            bucket_unit="minute",
            bucket_size=15,
        )

        response = fetch_upat_rollup_history(
            "upat_measurements_5min",
            "portable-test",
            params,
        )

        self.assertEqual(response["count"], 4)
        self.assertEqual(
            [item["event_time"] for item in response["items"]],
            [
                datetime(2026, 7, 1, 12, 45),
                datetime(2026, 7, 1, 12, 30),
                datetime(2026, 7, 1, 12, 15),
                datetime(2026, 7, 1, 12, 0),
            ],
        )
        self.assertEqual(
            [
                item["measurements"]["temperature"]["value"]
                for item in response["items"]
            ],
            [10.0, 7.0, 4.0, 1.0],
        )


if __name__ == "__main__":
    unittest.main()
