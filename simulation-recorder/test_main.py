import unittest
from contextlib import redirect_stdout
from datetime import date, datetime, timezone
from io import StringIO
from unittest.mock import Mock, patch

import main


class SimulationRecorderRequestContractTests(unittest.TestCase):
    def test_school_scope_defaults_to_supported_school_catalog(self):
        self.assertEqual(
            main.parse_simulation_school_ids(None),
            main.DEFAULT_SIMULATION_SCHOOL_IDS,
        )

    def test_school_scope_accepts_one_explicit_school(self):
        self.assertEqual(
            main.parse_simulation_school_ids("school_10"),
            ["school_10"],
        )

    def test_school_scope_trims_and_deduplicates(self):
        self.assertEqual(
            main.parse_simulation_school_ids(
                " school_10,school_10, school_22 "
            ),
            ["school_10", "school_22"],
        )

    def test_school_scope_rejects_empty_or_unsupported_values(self):
        for raw_value, expected_message in (
            ("  , ", "at least one school id"),
            ("school_10,school_typo", "unsupported school ids: school_typo"),
        ):
            with (
                self.subTest(raw_value=raw_value),
                self.assertRaisesRegex(ValueError, expected_message),
            ):
                main.parse_simulation_school_ids(raw_value)

    def test_request_contains_explicit_d_plus_one_target_date(self):
        self.assertEqual(
            main.build_simulation_request_body("school_10", date(2026, 8, 6)),
            {
                "school_id": "school_10",
                "target_date": "2026-08-06",
            },
        )

    @patch.object(main, "fetch_simulation_response")
    @patch.object(main, "create_day_ahead_run")
    @patch.object(
        main,
        "find_successful_day_ahead_run_id",
        return_value=543,
    )
    @patch.object(
        main,
        "utc_now",
        return_value=datetime(2026, 8, 5, 9, 0, tzinfo=timezone.utc),
    )
    def test_existing_successful_target_is_skipped_before_insert_and_http(
        self,
        _utc_now,
        find_existing,
        create_run,
        fetch_response,
    ):
        connection = Mock()
        output = StringIO()

        with redirect_stdout(output):
            result = main.run_school(
                connection,
                "https://backend.example/simulate/day-ahead",
                "school_10",
                "temporary-token",
            )

        self.assertIsNone(result)
        find_existing.assert_called_once_with(
            connection,
            "school_10",
            date(2026, 8, 6),
        )
        create_run.assert_not_called()
        fetch_response.assert_not_called()
        self.assertIn("existing_successful_run_id=543", output.getvalue())

    @patch("main.requests.post")
    def test_login_returns_temporary_bearer_token(self, post):
        response = Mock(ok=True, status_code=200)
        response.json.return_value = {
            "access_token": "temporary-token",
            "token_type": "bearer",
        }
        post.return_value = response

        token = main.fetch_simulation_access_token(
            "https://backend.example/auth/login",
            "simulation_recorder",
            "service-password",
        )

        self.assertEqual(token, "temporary-token")
        post.assert_called_once_with(
            "https://backend.example/auth/login",
            json={
                "username": "simulation_recorder",
                "password": "service-password",
            },
            timeout=main.SIMULATION_AUTH_TIMEOUT_SECONDS,
        )

    @patch("main.requests.post")
    def test_simulation_request_uses_bearer_token(self, post):
        response = Mock(status_code=200)
        post.return_value = response

        returned = main.fetch_simulation_response(
            "https://backend.example/simulate/day-ahead",
            {"school_id": "school_10", "target_date": "2026-08-06"},
            "temporary-token",
        )

        self.assertIs(returned, response)
        post.assert_called_once_with(
            "https://backend.example/simulate/day-ahead",
            json={"school_id": "school_10", "target_date": "2026-08-06"},
            headers={"Authorization": "Bearer temporary-token"},
            timeout=(
                main.SIMULATION_CONNECT_TIMEOUT_SECONDS,
                main.SIMULATION_REQUEST_TIMEOUT_SECONDS,
            ),
        )

    @patch("main.requests.post")
    def test_transport_timeout_is_not_retried(self, post):
        post.side_effect = main.requests.ReadTimeout("response timed out")
        output = StringIO()

        with (
            redirect_stdout(output),
            self.assertRaises(main.requests.ReadTimeout),
        ):
            main.fetch_simulation_response(
                "https://backend.example/simulate/day-ahead",
                {"school_id": "school_10", "target_date": "2026-08-06"},
                "temporary-token",
            )

        post.assert_called_once()
        self.assertIn("server-side completion is unknown", output.getvalue())

    @patch("main.requests.post")
    def test_missing_credentials_fail_before_http_request(self, post):
        for username, password, variable_name in (
            ("", "password", "SIMULATION_API_USERNAME"),
            ("simulation_recorder", "", "SIMULATION_API_PASSWORD"),
        ):
            with (
                self.subTest(variable_name=variable_name),
                self.assertRaisesRegex(
                    main.SimulationAuthenticationError,
                    variable_name,
                ),
            ):
                main.fetch_simulation_access_token(
                    "https://backend.example/auth/login",
                    username,
                    password,
                )
        post.assert_not_called()

    @patch("main.requests.post")
    def test_rejected_login_does_not_expose_credentials(self, post):
        post.return_value = Mock(ok=False, status_code=401)
        password = "do-not-log-this-password"

        with self.assertRaises(main.SimulationAuthenticationError) as raised:
            main.fetch_simulation_access_token(
                "https://backend.example/auth/login",
                "simulation_recorder",
                password,
            )

        self.assertIn("HTTP 401", str(raised.exception))
        self.assertNotIn(password, str(raised.exception))

    @patch.object(main, "SIMULATION_API_PASSWORD", "do-not-log-this-password")
    @patch.object(main, "SIMULATION_API_USERNAME", "simulation_recorder")
    @patch.object(main, "fetch_simulation_access_token")
    @patch.object(main, "db_connect")
    def test_authentication_failure_stops_before_database_and_redacts_secrets(
        self,
        db_connect,
        fetch_access_token,
    ):
        fetch_access_token.side_effect = main.SimulationAuthenticationError(
            "Simulation API login returned HTTP 401"
        )
        output = StringIO()

        with (
            redirect_stdout(output),
            self.assertRaises(main.SimulationAuthenticationError),
        ):
            main.run()

        db_connect.assert_not_called()
        self.assertIn("authentication failed", output.getvalue())
        self.assertNotIn("simulation_recorder", output.getvalue())
        self.assertNotIn("do-not-log-this-password", output.getvalue())


if __name__ == "__main__":
    unittest.main()
