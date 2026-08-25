"""Minimal, fail-closed FusionSolar Northbound API client."""

from __future__ import annotations

import re
import time
from typing import Any
from urllib.parse import urlparse

import requests


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


class FusionSolarClient:
    """Sequential client with no implicit retries or payload logging."""

    def __init__(
        self,
        *,
        base_url: str,
        username: str,
        system_code: str,
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

        started = time.monotonic()
        try:
            response = self.session.post(
                f"{self.base_url}/{endpoint}",
                json=payload,
                headers=headers,
                timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
            )
        except requests.RequestException as exc:
            raise FusionSolarTransportError(
                f"{endpoint} transport failure: {type(exc).__name__}; no retry attempted"
            ) from exc

        body = _response_json(response, endpoint)
        fail_code = body.get("failCode")
        self.call_reports.append(
            {
                "endpoint": endpoint,
                "http_status": response.status_code,
                "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
                "response_bytes": len(response.content),
                "success": body.get("success"),
                "fail_code": fail_code,
                "automatic_retries": 0,
            }
        )

        if response.status_code == 429 or fail_code == 407:
            raise FusionSolarRateLimitError(
                f"{endpoint} was rate limited or blocked; immediate retries are disabled"
            )
        if response.status_code >= 400:
            error_type = (
                FusionSolarAuthenticationError
                if endpoint == "login" or response.status_code in {401, 403}
                else FusionSolarError
            )
            raise error_type(f"{endpoint} returned HTTP {response.status_code}")
        if body.get("success") is not True or fail_code not in (0, None):
            error_type = (
                FusionSolarAuthenticationError
                if endpoint == "login"
                else FusionSolarError
            )
            raise error_type(
                f"{endpoint} application failure failCode={fail_code}"
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
