import base64
import hashlib
import json
import unittest
from asyncio import run
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

import auth_service
import main
from fastapi import HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError


VALID_SERVICE_TOKEN = "service-" + "a" * 40
WRONG_SERVICE_TOKEN = "service-" + "b" * 40


def encoded_test_hash(password, *, iterations=100_000):
    salt = b"isolated-test-salt"
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        iterations,
    )
    encoded_salt = base64.urlsafe_b64encode(salt).decode("ascii").rstrip("=")
    encoded_digest = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return f"pbkdf2_sha256${iterations}${encoded_salt}${encoded_digest}"


def user_row(**overrides):
    row = {
        "username": "school_10",
        "password_hash": "stored-password-hash",
        "role": "teacher",
        "school_id": "school_10",
        "municipality_id": None,
        "school_ids": ["school_10"],
        "is_active": True,
        "token_version": 1,
    }
    row.update(overrides)
    return row


class _FakeCursor:
    def __init__(self, row):
        self.row = row
        self.query = None
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, params):
        self.query = query
        self.params = params

    def fetchone(self):
        return self.row


class _FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def cursor(self):
        return self._cursor


class _FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class AuthServiceBoundaryTests(unittest.TestCase):
    def setUp(self):
        main.AUTH_VERIFY_RATE_LIMITER.reset()

    def tearDown(self):
        main.AUTH_VERIFY_RATE_LIMITER.reset()

    @staticmethod
    def request_for(path):
        return Request(
            {
                "type": "http",
                "http_version": "1.1",
                "method": "POST",
                "scheme": "https",
                "path": path,
                "raw_path": path.encode("ascii"),
                "query_string": b"",
                "headers": [],
                "client": ("127.0.0.1", 12345),
                "server": ("testserver", 443),
            }
        )

    @staticmethod
    def oversized_password_error(password):
        return RequestValidationError(
            [
                {
                    "type": "string_too_long",
                    "loc": ("body", "password"),
                    "msg": "String should have at most 256 characters",
                    "input": password,
                    "ctx": {"max_length": 256},
                }
            ]
        )

    def route(self, path):
        candidates = []
        for route in main.app.routes:
            candidates.append(route)
            original_router = getattr(route, "original_router", None)
            if original_router is not None:
                candidates.extend(original_router.routes)
        return next(
            route
            for route in candidates
            if getattr(route, "path", None) == path
        )

    def call_route(self, path, payload, *, token=VALID_SERVICE_TOKEN, row=None):
        cursor = _FakeCursor(row)
        route = self.route(path)
        dependency = route.dependant.dependencies[0].call
        authorization = f"Bearer {token}" if token is not None else None
        with (
            patch.object(main, "AUTH_SERVICE_TOKEN", VALID_SERVICE_TOKEN),
            patch.object(main, "get_connection", return_value=_FakeConnection(cursor)),
        ):
            dependency(authorization)
            response = Response()
            request_model = (
                auth_service.VerifyCredentialsRequest
                if path.endswith("/verify")
                else auth_service.UsernameRequest
            )
            result = route.endpoint(request_model(**payload), response)
        return result, response, cursor

    def test_unconfigured_service_token_fails_closed_before_database_access(self):
        route = self.route("/internal/auth/resolve")
        dependency = route.dependant.dependencies[0].call
        with (
            patch.object(main, "AUTH_SERVICE_TOKEN", ""),
            patch.object(main, "get_connection") as get_connection,
        ):
            with self.assertRaises(HTTPException) as context:
                dependency(f"Bearer {VALID_SERVICE_TOKEN}")

        self.assertEqual(context.exception.status_code, 503)
        get_connection.assert_not_called()

    def test_password_verifier_accepts_only_the_matching_bounded_pbkdf2_hash(self):
        encoded = encoded_test_hash("unit-test-password")

        self.assertTrue(auth_service.verify_password("unit-test-password", encoded))
        self.assertFalse(auth_service.verify_password("wrong-password", encoded))
        self.assertFalse(
            auth_service.verify_password(
                "unit-test-password",
                encoded_test_hash("unit-test-password", iterations=99_999),
            )
        )
        self.assertFalse(
            auth_service.verify_password(
                "unit-test-password",
                "pbkdf2_sha256$2000001$invalid$invalid",
            )
        )

    def test_missing_or_wrong_service_token_is_rejected(self):
        dependency = self.route(
            "/internal/auth/resolve"
        ).dependant.dependencies[0].call
        with patch.object(main, "AUTH_SERVICE_TOKEN", VALID_SERVICE_TOKEN):
            with self.assertRaises(HTTPException) as missing:
                dependency(None)
            with self.assertRaises(HTTPException) as wrong:
                dependency(f"Bearer {WRONG_SERVICE_TOKEN}")

        self.assertEqual(missing.exception.status_code, 401)
        self.assertEqual(wrong.exception.status_code, 401)
        self.assertEqual(missing.exception.headers["WWW-Authenticate"], "Bearer")

    @patch.object(auth_service, "verify_password", return_value=False)
    def test_verify_rate_limit_blocks_before_database_and_recovers_after_window(
        self,
        _verify_password,
    ):
        clock = _FakeClock()
        limiter = auth_service.AuthVerifyRateLimiter(
            attempts_per_username=2,
            global_attempts=10,
            window_seconds=60,
            clock=clock,
        )
        connection_factory = Mock(
            return_value=_FakeConnection(_FakeCursor(row=None))
        )
        router = auth_service.build_auth_router(
            connection_factory=connection_factory,
            service_token_getter=lambda: VALID_SERVICE_TOKEN,
            verify_rate_limiter=limiter,
        )
        route = next(
            route
            for route in router.routes
            if getattr(route, "path", None) == "/internal/auth/verify"
        )
        dependency = route.dependant.dependencies[0].call

        for username in (" school_10 ", "school_10"):
            dependency(f"Bearer {VALID_SERVICE_TOKEN}")
            with self.assertRaises(HTTPException) as rejected:
                route.endpoint(
                    auth_service.VerifyCredentialsRequest(
                        username=username,
                        password="private-value",
                    ),
                    Response(),
                )
            self.assertEqual(rejected.exception.status_code, 401)

        with self.assertRaises(HTTPException) as limited:
            route.endpoint(
                auth_service.VerifyCredentialsRequest(
                    username="school_10",
                    password="private-value",
                ),
                Response(),
            )

        self.assertEqual(limited.exception.status_code, 429)
        self.assertEqual(limited.exception.headers["Retry-After"], "60")
        self.assertEqual(limited.exception.headers["Cache-Control"], "no-store")
        self.assertEqual(connection_factory.call_count, 2)

        clock.advance(60)
        with self.assertRaises(HTTPException) as after_window:
            route.endpoint(
                auth_service.VerifyCredentialsRequest(
                    username="school_10",
                    password="private-value",
                ),
                Response(),
            )

        self.assertEqual(after_window.exception.status_code, 401)
        self.assertEqual(connection_factory.call_count, 3)

    def test_verify_global_rate_limit_bounds_distinct_usernames(self):
        clock = _FakeClock()
        limiter = auth_service.AuthVerifyRateLimiter(
            attempts_per_username=5,
            global_attempts=2,
            window_seconds=60,
            clock=clock,
        )

        limiter.check("school_10")
        limiter.check("school_22")
        with self.assertRaises(HTTPException) as limited:
            limiter.check("previously-unseen-user")

        self.assertEqual(limited.exception.status_code, 429)
        self.assertEqual(limited.exception.headers["Retry-After"], "60")

        clock.advance(60)
        limiter.check("previously-unseen-user")

    def test_verify_global_rate_limit_is_atomic_under_concurrency(self):
        limiter = auth_service.AuthVerifyRateLimiter(
            attempts_per_username=40,
            global_attempts=10,
            window_seconds=60,
            clock=lambda: 0.0,
        )

        def attempt(index):
            try:
                limiter.check(f"concurrent-user-{index}")
            except HTTPException as error:
                self.assertEqual(error.status_code, 429)
                return False
            return True

        with ThreadPoolExecutor(max_workers=20) as executor:
            accepted = list(executor.map(attempt, range(40)))

        self.assertEqual(sum(accepted), 10)

    @patch.object(auth_service, "verify_password", return_value=True)
    def test_verify_returns_public_identity_without_hash(self, verify_password):
        result, response, cursor = self.call_route(
            "/internal/auth/verify",
            {"username": " school_10 ", "password": "private-value"},
            row=user_row(),
        )

        self.assertEqual(
            result,
            {
                "username": "school_10",
                "role": "teacher",
                "school_id": "school_10",
                "municipality_id": None,
                "school_ids": ["school_10"],
                "token_version": 1,
            },
        )
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertNotIn("password", str(result).lower())
        self.assertEqual(cursor.params, ("school_10",))
        self.assertNotIn("private-value", str(cursor.params))
        verify_password.assert_called_once_with("private-value", "stored-password-hash")

    @patch.object(auth_service, "verify_password", return_value=False)
    def test_verify_uses_the_same_error_for_missing_user_and_wrong_password(
        self,
        _verify_password,
    ):
        with self.assertRaises(HTTPException) as missing:
            self.call_route(
                "/internal/auth/verify",
                {"username": "missing", "password": "private-value"},
                row=None,
            )
        with self.assertRaises(HTTPException) as wrong:
            self.call_route(
                "/internal/auth/verify",
                {"username": "school_10", "password": "private-value"},
                row=user_row(),
            )

        self.assertEqual(missing.exception.status_code, 401)
        self.assertEqual(wrong.exception.status_code, 401)
        self.assertEqual(missing.exception.detail, wrong.exception.detail)

    def test_resolve_returns_active_identity_without_querying_password_hash(self):
        result, response, cursor = self.call_route(
            "/internal/auth/resolve",
            {"username": "school_10"},
            row=user_row(),
        )

        self.assertNotIn("password_hash", cursor.query)
        self.assertEqual(result["token_version"], 1)
        self.assertEqual(response.headers["cache-control"], "no-store")

    def test_resolve_hides_missing_and_inactive_identity_details(self):
        with self.assertRaises(HTTPException) as missing:
            self.call_route(
                "/internal/auth/resolve",
                {"username": "missing"},
                row=None,
            )
        with self.assertRaises(HTTPException) as inactive:
            self.call_route(
                "/internal/auth/resolve",
                {"username": "school_10"},
                row=user_row(is_active=False),
            )

        self.assertEqual(missing.exception.status_code, 404)
        self.assertEqual(inactive.exception.status_code, 404)
        self.assertEqual(missing.exception.detail, inactive.exception.detail)

    def test_invalid_stored_scope_fails_closed_without_leaking_details(self):
        with self.assertRaises(HTTPException) as response:
            self.call_route(
                "/internal/auth/resolve",
                {"username": "school_10"},
                row=user_row(school_ids=["school_10", "school_22"]),
            )

        self.assertEqual(response.exception.status_code, 503)
        self.assertEqual(
            response.exception.detail,
            "Authentication service is temporarily unavailable.",
        )

    def test_auth_validation_error_does_not_echo_password_and_is_not_cacheable(self):
        password = "private-marker-" + "x" * 260
        response = run(
            main.redact_auth_request_validation_error(
                self.request_for("/internal/auth/verify"),
                self.oversized_password_error(password),
            )
        )
        body = json.loads(response.body)

        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertEqual(body, {"detail": "Invalid authentication request."})
        self.assertNotIn(password, response.body.decode("utf-8"))

    def test_non_auth_validation_errors_keep_the_default_fastapi_shape(self):
        rejected_value = "ordinary-invalid-value"
        response = run(
            main.redact_auth_request_validation_error(
                self.request_for("/weather/forecast"),
                self.oversized_password_error(rejected_value),
            )
        )
        body = json.loads(response.body)

        self.assertEqual(response.status_code, 422)
        self.assertEqual(body["detail"][0]["input"], rejected_value)


if __name__ == "__main__":
    unittest.main()
