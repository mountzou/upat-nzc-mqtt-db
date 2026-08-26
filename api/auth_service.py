from __future__ import annotations

import base64
import hashlib
import hmac
import math
import time
from collections import deque
from collections.abc import Callable
from threading import Lock
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from pydantic import BaseModel, Field, field_validator


MIN_SERVICE_TOKEN_LENGTH = 32
MIN_PBKDF2_ITERATIONS = 100_000
MAX_PBKDF2_ITERATIONS = 2_000_000
VERIFY_RATE_LIMIT_ATTEMPTS_PER_USERNAME = 5
VERIFY_RATE_LIMIT_GLOBAL_ATTEMPTS = 60
VERIFY_RATE_LIMIT_WINDOW_SECONDS = 60
SUPPORTED_AUTH_ROLES = frozenset({"teacher", "municipality", "system_admin"})
_DUMMY_PASSWORD_HASH = (
    "pbkdf2_sha256$600000$av9ekFPpUrAoJhJbySQ9GQ$"
    "qtkFqEagEogxOON8nPMRRNn84agyFSui_gZCa2U2ngc"
)


class UsernameRequest(BaseModel):
    username: str = Field(min_length=1, max_length=120)

    @field_validator("username")
    @classmethod
    def normalize_username(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("username must not be blank")
        return normalized


class VerifyCredentialsRequest(UsernameRequest):
    password: str = Field(min_length=1, max_length=256)


class AuthUserResponse(BaseModel):
    username: str
    role: Literal["teacher", "municipality", "system_admin"]
    school_id: str | None = None
    municipality_id: str | None = None
    school_ids: list[str] = Field(default_factory=list)
    token_version: int = Field(ge=1)


def _decode_base64url(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def verify_password(password: str, encoded_hash: str) -> bool:
    try:
        algorithm, iterations_text, salt_text, expected_text = encoded_hash.split("$", 3)
        iterations = int(iterations_text)
        if algorithm != "pbkdf2_sha256" or not (
            MIN_PBKDF2_ITERATIONS <= iterations <= MAX_PBKDF2_ITERATIONS
        ):
            return False
        salt = _decode_base64url(salt_text)
        expected = _decode_base64url(expected_text)
        if len(salt) < 16 or len(expected) < 32:
            return False
    except (TypeError, ValueError):
        return False

    actual = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        iterations,
    )
    return hmac.compare_digest(actual, expected)


def _service_auth_error(status_code: int, detail: str) -> HTTPException:
    return HTTPException(
        status_code=status_code,
        detail=detail,
        headers={
            "WWW-Authenticate": "Bearer",
            "Cache-Control": "no-store",
        },
    )


def _credential_error() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid username or password.",
        headers={"Cache-Control": "no-store"},
    )


def _unavailable_error() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="Authentication service is temporarily unavailable.",
        headers={"Cache-Control": "no-store"},
    )


def _rate_limit_error(retry_after_seconds: int) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail="Too many authentication attempts. Try again later.",
        headers={
            "Cache-Control": "no-store",
            "Retry-After": str(retry_after_seconds),
        },
    )


class AuthVerifyRateLimiter:
    """Thread-safe rolling-window limiter for password verification attempts."""

    def __init__(
        self,
        *,
        attempts_per_username: int = VERIFY_RATE_LIMIT_ATTEMPTS_PER_USERNAME,
        global_attempts: int = VERIFY_RATE_LIMIT_GLOBAL_ATTEMPTS,
        window_seconds: int = VERIFY_RATE_LIMIT_WINDOW_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if attempts_per_username < 1 or global_attempts < 1 or window_seconds < 1:
            raise ValueError("auth rate-limit settings must be positive")

        self._attempts_per_username = attempts_per_username
        self._global_attempts = global_attempts
        self._window_seconds = window_seconds
        self._clock = clock
        self._events_by_username: dict[bytes, deque[float]] = {}
        self._global_events: deque[float] = deque()
        self._lock = Lock()

    @staticmethod
    def _prune(events: deque[float], cutoff: float) -> None:
        while events and events[0] <= cutoff:
            events.popleft()

    def _retry_after(self, events: deque[float], now: float) -> int:
        return max(1, math.ceil(self._window_seconds - (now - events[0])))

    def check(self, username: str) -> None:
        now = self._clock()
        cutoff = now - self._window_seconds
        username_key = hashlib.sha256(username.encode("utf-8")).digest()

        with self._lock:
            self._prune(self._global_events, cutoff)
            for key, events in list(self._events_by_username.items()):
                self._prune(events, cutoff)
                if not events:
                    del self._events_by_username[key]

            username_events = self._events_by_username.setdefault(
                username_key,
                deque(),
            )
            if len(username_events) >= self._attempts_per_username:
                raise _rate_limit_error(self._retry_after(username_events, now))
            if len(self._global_events) >= self._global_attempts:
                raise _rate_limit_error(self._retry_after(self._global_events, now))

            username_events.append(now)
            self._global_events.append(now)

    def reset(self) -> None:
        with self._lock:
            self._events_by_username.clear()
            self._global_events.clear()


def _public_user(row: dict[str, Any]) -> dict[str, Any]:
    username = str(row.get("username", "")).strip()
    role = str(row.get("role", "")).strip()
    school_id = row.get("school_id")
    municipality_id = row.get("municipality_id")
    school_ids = list(row.get("school_ids") or [])
    token_version = row.get("token_version")

    if not username or role not in SUPPORTED_AUTH_ROLES:
        raise ValueError("invalid stored auth identity")
    if not isinstance(token_version, int) or token_version < 1:
        raise ValueError("invalid stored token version")
    if len(school_ids) != len(set(school_ids)) or any(
        not isinstance(item, str) or not item for item in school_ids
    ):
        raise ValueError("invalid stored school scope")
    if role == "teacher" and not (
        school_id and municipality_id is None and school_ids == [school_id]
    ):
        raise ValueError("invalid stored teacher scope")
    if role == "municipality" and not (
        school_id is None and municipality_id and school_ids
    ):
        raise ValueError("invalid stored municipality scope")
    if role == "system_admin" and not (
        school_id is None and municipality_id is None and not school_ids
    ):
        raise ValueError("invalid stored system administrator scope")

    return {
        "username": username,
        "role": role,
        "school_id": school_id,
        "municipality_id": municipality_id,
        "school_ids": school_ids,
        "token_version": token_version,
    }


def build_auth_router(
    *,
    connection_factory: Callable[[], Any],
    service_token_getter: Callable[[], str],
    verify_rate_limiter: AuthVerifyRateLimiter,
) -> APIRouter:
    router = APIRouter(prefix="/internal/auth", tags=["internal-auth"])

    def require_auth_service_token(
        authorization: Annotated[
            str | None,
            Header(alias="Authorization"),
        ] = None,
    ) -> None:
        configured_token = service_token_getter().strip()
        if len(configured_token) < MIN_SERVICE_TOKEN_LENGTH:
            raise _service_auth_error(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "Authentication service is not configured.",
            )

        scheme, separator, credentials = (authorization or "").partition(" ")
        if (
            not separator
            or scheme.lower() != "bearer"
            or not hmac.compare_digest(credentials, configured_token)
        ):
            raise _service_auth_error(
                status.HTTP_401_UNAUTHORIZED,
                "Invalid or missing service credentials.",
            )

    def fetch_user(username: str, *, include_password_hash: bool) -> dict[str, Any] | None:
        if include_password_hash:
            query = """
                SELECT
                    username,
                    role,
                    school_id,
                    municipality_id,
                    school_ids,
                    is_active,
                    token_version,
                    password_hash
                FROM app_users
                WHERE username = %s
                LIMIT 1;
            """
        else:
            query = """
                SELECT
                    username,
                    role,
                    school_id,
                    municipality_id,
                    school_ids,
                    is_active,
                    token_version
                FROM app_users
                WHERE username = %s
                LIMIT 1;
            """

        with connection_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query,
                    (username,),
                )
                return cur.fetchone()

    @router.post(
        "/verify",
        response_model=AuthUserResponse,
        dependencies=[Depends(require_auth_service_token)],
    )
    def verify_credentials(
        payload: VerifyCredentialsRequest,
        response: Response,
    ) -> dict[str, Any]:
        response.headers["Cache-Control"] = "no-store"
        verify_rate_limiter.check(payload.username)
        try:
            row = fetch_user(payload.username, include_password_hash=True)
            encoded_hash = row.get("password_hash") if row else _DUMMY_PASSWORD_HASH
            valid_password = verify_password(payload.password, encoded_hash)
            if row is None or not row.get("is_active") or not valid_password:
                raise _credential_error()
            return _public_user(row)
        except HTTPException:
            raise
        except Exception:
            raise _unavailable_error() from None

    @router.post(
        "/resolve",
        response_model=AuthUserResponse,
        dependencies=[Depends(require_auth_service_token)],
    )
    def resolve_user(
        payload: UsernameRequest,
        response: Response,
    ) -> dict[str, Any]:
        response.headers["Cache-Control"] = "no-store"
        try:
            row = fetch_user(payload.username, include_password_hash=False)
            if row is None or not row.get("is_active"):
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Authentication identity is unavailable.",
                    headers={"Cache-Control": "no-store"},
                )
            return _public_user(row)
        except HTTPException:
            raise
        except Exception:
            raise _unavailable_error() from None

    return router
