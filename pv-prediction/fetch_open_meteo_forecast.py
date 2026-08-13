"""Fetch hourly Open-Meteo forecast variables for PV feature engineering."""
from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARS = [
    "temperature_2m",
    "cloud_cover",
    "shortwave_radiation",
    "direct_normal_irradiance",
    "diffuse_radiation",
    "wind_speed_10m",
]

DEFAULT_LAT = 37.04
DEFAULT_LON = 22.11
FORECAST_DAYS = 1
TIMEZONE = "auto"
DEFAULT_MAX_ATTEMPTS = 4
DEFAULT_BACKOFF_BASE_SECONDS = 10.0
DEFAULT_BACKOFF_MAX_SECONDS = 40.0
RETRYABLE_HTTP_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}


def build_forecast_url(
    latitude: float = DEFAULT_LAT,
    longitude: float = DEFAULT_LON,
    *,
    forecast_days: int = FORECAST_DAYS,
    timezone: str = TIMEZONE,
) -> str:
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(HOURLY_VARS),
        "forecast_days": forecast_days,
        "timezone": timezone,
    }
    return f"{BASE}?{urllib.parse.urlencode(params)}"


def _retry_delay_seconds(
    attempt: int,
    *,
    backoff_base_s: float,
    backoff_max_s: float,
) -> float:
    return min(backoff_base_s * (2 ** (attempt - 1)), backoff_max_s)


def fetch_forecast_json(
    url: str,
    timeout_s: float = 30.0,
    *,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    backoff_base_s: float = DEFAULT_BACKOFF_BASE_SECONDS,
    backoff_max_s: float = DEFAULT_BACKOFF_MAX_SECONDS,
    sleep_fn=time.sleep,
) -> dict:
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    if backoff_base_s < 0 or backoff_max_s < 0:
        raise ValueError("backoff delays must be non-negative")

    req = urllib.request.Request(url, headers={"User-Agent": "pv-prediction/1.0"})
    for attempt in range(1, max_attempts + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if (
                exc.code not in RETRYABLE_HTTP_STATUS_CODES
                or attempt == max_attempts
            ):
                raise
            reason = f"HTTP {exc.code} {exc.reason}"
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt == max_attempts:
                raise
            reason = f"network error: {exc}"

        delay_s = _retry_delay_seconds(
            attempt,
            backoff_base_s=backoff_base_s,
            backoff_max_s=backoff_max_s,
        )
        print(
            "Open-Meteo request failed "
            f"({reason}); retrying in {delay_s:.1f}s "
            f"(attempt {attempt + 1}/{max_attempts})",
            file=sys.stderr,
        )
        sleep_fn(delay_s)

    raise RuntimeError("Open-Meteo retry loop exited unexpectedly")


def validate_hourly_payload(data: dict) -> None:
    if "hourly" not in data:
        raise ValueError("Response missing 'hourly'")
    hourly = data["hourly"]
    times = hourly.get("time")
    if not times:
        raise ValueError("hourly.time is empty or missing")
    for var in HOURLY_VARS:
        if var not in hourly:
            raise ValueError(f"hourly missing variable: {var}")
        if len(hourly[var]) != len(times):
            raise ValueError(f"length mismatch for {var}: {len(hourly[var])} vs {len(times)}")


def main() -> int:
    url = build_forecast_url()
    print("GET", url, "\n")
    try:
        data = fetch_forecast_json(url)
    except urllib.error.HTTPError as e:
        print("HTTP error:", e.code, e.reason, file=sys.stderr)
        return 1
    except urllib.error.URLError as e:
        print("URL error:", e.reason, file=sys.stderr)
        return 1
    except json.JSONDecodeError as e:
        print("Invalid JSON:", e, file=sys.stderr)
        return 1

    try:
        validate_hourly_payload(data)
    except ValueError as e:
        print("Validation error:", e, file=sys.stderr)
        return 1

    hourly = data["hourly"]
    n = len(hourly["time"])
    print(f"OK: received {n} hourly rows")
    print(f"  time[0] .. time[-1]: {hourly['time'][0]} .. {hourly['time'][-1]}")
    i = 0
    print(f"  sample hour {hourly['time'][i]}:")
    for var in HOURLY_VARS:
        print(f"    {var}: {hourly[var][i]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
