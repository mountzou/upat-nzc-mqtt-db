"""Minimal, fail-closed FusionSolar Northbound API client."""

from __future__ import annotations

import re
import time
import math
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlparse

import requests

from api_control import ApiControl


CONNECT_TIMEOUT_SECONDS = 10
READ_TIMEOUT_SECONDS = 90


class FusionSolarError(RuntimeError):
    """Base error for a failed FusionSolar call."""


class FusionSolarAuthenticationError(FusionSolarError):
    """Authentication failed or returned no reusable token."""


class FusionSolarRateLimitError(FusionSolarError):
    """FusionSolar rejected the call due to throttling or account blocking."""


class FusionSolarTransportError(FusionSolarError):
    """The request could not complete at the HTTP transport layer."""


def validate_base_url(base_url: str) -> str:
    normalized = base_url.strip().rstrip("/")
    parsed = urlparse(normalized)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or not parsed.path.endswith("/thirdData")
    ):
        raise ValueError(
            "FUSIONSOLAR_BASE_URL must be an HTTPS FusionSolar /thirdData endpoint"
        )
    return normalized


def _response_json(response: requests.Response, endpoint: str) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError as exc:
        raise FusionSolarError(
            f"{endpoint} returned non-JSON content with HTTP {response.status_code}"
        ) from exc
    if not isinstance(body, dict):
        raise FusionSolarError(f"{endpoint} returned a non-object JSON payload")
    return body


def _extract_token(response: requests.Response) -> str | None:
    direct = response.headers.get("XSRF-TOKEN")
    if direct:
        return direct
    cookie = response.cookies.get("XSRF-TOKEN")
    if cookie:
        return cookie
    match = re.search(
        r"(?:^|[,;]\s*)XSRF-TOKEN=([^;,]+)",
        response.headers.get("set-cookie", ""),
    )
    return match.group(1) if match else None


def _retry_after(value: str | None, now: float) -> float | None:
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            seconds = parsedate_to_datetime(value).timestamp() - now
        except (TypeError, ValueError, OverflowError):
            return None
    return max(0, seconds) if math.isfinite(seconds) else None


class FusionSolarClient:
    """Sequential client with no implicit retries or payload logging."""

    def __init__(
        self,
        *,
        base_url: str,
        username: str,
        system_code: str,
        control: ApiControl,
        session: requests.Session | None = None,
    ) -> None:
        if not username or not system_code:
            raise ValueError("FusionSolar username and system code are required")
        self.base_url = validate_base_url(base_url)
        self.username = username
        self.system_code = system_code
        self.session = session or requests.Session()
        self.token: str | None = None
        self.call_reports: list[dict[str, Any]] = []
        self.control = control

    def _request(
        self,
        endpoint: str,
        payload: dict[str, Any],
        *,
        authenticated: bool,
    ) -> tuple[dict[str, Any], requests.Response]:
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if authenticated:
            if not self.token:
                raise FusionSolarAuthenticationError(
                    "login must complete before authenticated data calls"
                )
            headers["XSRF-TOKEN"] = self.token

        attempt = self.control.begin_attempt(endpoint, payload)
        started = time.monotonic()
        result = {"endpoint": endpoint, "http_status": None, "success": False,
                  "fail_code": None, "response_bytes": 0, "automatic_retries": 0,
                  "retry_after_seconds": None, "outcome": "transport_error"}
        try:
            response = self.session.post(
                f"{self.base_url}/{endpoint}",
                json=payload,
                headers=headers,
                timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
                allow_redirects=False,
            )
        except requests.RequestException as exc:
            result.update(elapsed_ms=round((time.monotonic() - started) * 1000, 1),
                          error_type=type(exc).__name__)
            self.call_reports.append(self.control.finish_attempt(attempt, result))
            raise FusionSolarTransportError(
                f"{endpoint} transport failure: {type(exc).__name__}; no retry attempted"
            ) from exc
        result.update(http_status=response.status_code, response_bytes=len(response.content),
                      retry_after_seconds=_retry_after(response.headers.get("Retry-After"), self.control.clock()))
        try:
            body = _response_json(response, endpoint)
        except FusionSolarError:
            body = None
        # Never log provider-supplied messages, identifiers, headers, or raw payloads.
        fail_code = body.get("failCode") if body else None
        if type(fail_code) is not int:
            fail_code = None
        result["fail_code"] = fail_code
        error_type = FusionSolarError
        if fail_code == 407:
            result["outcome"] = "account_rate_limit"
            error_type = FusionSolarRateLimitError
        elif response.status_code == 429 or fail_code == 429:
            result["outcome"] = "system_rate_limit"
            error_type = FusionSolarRateLimitError
        elif 300 <= response.status_code < 400:
            result["outcome"] = "redirect_refused"
        elif response.status_code >= 400:
            result["outcome"] = "http_error"
            if endpoint == "login" or response.status_code in {401, 403}:
                error_type = FusionSolarAuthenticationError
        elif body is None:
            result["outcome"] = "invalid_json"
        elif body.get("success") is not True or body.get("failCode") not in (0, None):
            result["outcome"] = "application_error"
            if endpoint == "login":
                error_type = FusionSolarAuthenticationError
        elif endpoint == "login" and not _extract_token(response):
            result["outcome"] = "missing_token"
            error_type = FusionSolarAuthenticationError
        else:
            result.update(outcome="success", success=True)
        result["elapsed_ms"] = round((time.monotonic() - started) * 1000, 1)
        self.call_reports.append(self.control.finish_attempt(attempt, result))
        if result["outcome"] != "success":
            reason = ("was rate limited or blocked; immediate retries are disabled; "
                      if error_type is FusionSolarRateLimitError else "")
            raise error_type(
                f"{endpoint} {reason}{result['outcome']}: HTTP={response.status_code}, "
                f"failCode={fail_code}, devTypeId={attempt.get('dev_type_id')}; no retry attempted"
            )
        return body, response

    def login(self) -> None:
        if self.token is not None:
            raise FusionSolarAuthenticationError(
                "this client already authenticated; repeated login is disabled"
            )
        body, response = self._request(
            "login",
            {"userName": self.username, "systemCode": self.system_code},
            authenticated=False,
        )
        token = _extract_token(response)
        if not token:
            raise FusionSolarAuthenticationError(
                f"login succeeded={body.get('success')} but returned no XSRF token"
            )
        self.token = token

    def get_device_list(self, plant_code: str) -> dict[str, Any]:
        if not plant_code.strip():
            raise ValueError("FusionSolar plant code is required")
        body, _ = self._request(
            "getDevList",
            {"stationCodes": plant_code.strip()},
            authenticated=True,
        )
        return body
    def get_history(
        self,
        *,
        device_ids: list[str],
        device_type: int,
        start_ms: int,
        end_ms: int,
    ) -> dict[str, Any]:
        if not device_ids:
            raise ValueError("at least one device ID is required")
        if len(device_ids) > 10:
            raise ValueError("FusionSolar historical calls support at most 10 devices")
        if start_ms > end_ms:
            raise ValueError("history start must be before or equal to end")
        body, _ = self._request(
            "getDevHistoryKpi",
            {
                "devIds": ",".join(device_ids),
                "devTypeId": device_type,
                "startTime": start_ms,
                "endTime": end_ms,
            },
            authenticated=True,
        )
        return body
