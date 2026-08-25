"""Contract checks for the local-only PV shadow Compose service."""

from __future__ import annotations

import json
import subprocess
import unittest
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOCAL_COMPOSE = ROOT / "docker-compose.yml"
PRODUCTION_COMPOSE = ROOT / "docker-compose.prod.yml"
SHADOW_SERVICE = "pv-shadow-prediction"

POSTGRES_ENV_KEYS = {
    "POSTGRES_HOST",
    "POSTGRES_INTERNAL_PORT",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
}


def resolved_compose(path: Path) -> dict[str, Any]:
    """Resolve Compose structure without loading .env values or contacting Docker."""
    command = [
        "docker",
        "compose",
        "--env-file",
        "/dev/null",
        "-f",
        str(path),
        "config",
        "--format",
        "json",
        "--no-interpolate",
        "--no-path-resolution",
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise unittest.SkipTest("Docker Compose is required for this contract test") from exc

    if completed.returncode != 0:
        raise AssertionError(
            f"Could not resolve {path.name}: {completed.stderr.strip()}"
        )

    return json.loads(completed.stdout)


class PvShadowComposeContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.local = resolved_compose(LOCAL_COMPOSE)
        cls.production = resolved_compose(PRODUCTION_COMPOSE)

    def shadow_service(self) -> dict[str, Any]:
        services = self.local.get("services", {})
        shadow = services.get(SHADOW_SERVICE)
        if shadow is None:
            self.fail("docker-compose.yml must declare the local PV shadow service")
        return shadow

    def test_shadow_service_is_local_only(self) -> None:
        self.assertNotIn(
            SHADOW_SERVICE,
            self.production.get("services", {}),
            "docker-compose.prod.yml must not contain the shadow service",
        )

    def test_shadow_service_requires_explicit_shadow_profile(self) -> None:
        shadow = self.shadow_service()
        self.assertEqual(["shadow"], shadow.get("profiles"))

    def test_shadow_service_exposes_no_host_surface(self) -> None:
        shadow = self.shadow_service()
        self.assertNotIn("ports", shadow)
        self.assertNotIn("container_name", shadow)

    def test_shadow_service_uses_its_own_runner_without_open_meteo(self) -> None:
        shadow = self.shadow_service()
        build = shadow.get("build", {})
        context = str(build.get("context", "")).rstrip("/")
        self.assertEqual(
            "./pv-shadow-prediction",
            context,
            "the shadow lane must use its own runner, not pv-prediction",
        )

        rendered = json.dumps(shadow, sort_keys=True).lower()
        for forbidden in (
            "open_meteo",
            "open-meteo",
            "api.open-meteo.com",
            "weather-collector",
        ):
            self.assertNotIn(
                forbidden,
                rendered,
                "the shadow lane must reuse persisted champion inputs instead of "
                "fetching weather again",
            )

    def test_shadow_service_has_only_database_integration_credentials(self) -> None:
        shadow = self.shadow_service()
        environment = shadow.get("environment", {})
        self.assertTrue(
            POSTGRES_ENV_KEYS.issubset(environment),
            f"missing PostgreSQL settings: {sorted(POSTGRES_ENV_KEYS - set(environment))}",
        )

        integration_prefixes = (
            "MQTT_",
            "TTN_",
            "SIMULATION_",
            "OPEN_METEO_",
            "OPS_",
        )
        unexpected = sorted(
            key
            for key in environment
            if key.startswith(integration_prefixes)
        )
        self.assertEqual([], unexpected)

        dependencies = set(shadow.get("depends_on", {}))
        self.assertEqual(
            {"postgres"},
            dependencies,
            "the shadow lane may depend only on PostgreSQL",
        )

    def test_shadow_artifact_mounts_are_existing_read_only_files(self) -> None:
        shadow = self.shadow_service()
        volumes = shadow.get("volumes", [])
        self.assertEqual(4, len(volumes))
        for volume in volumes:
            source = str(volume.get("source", ""))
            source_path = ROOT / (source[2:] if source.startswith("./") else source)
            self.assertTrue(source_path.is_file(), source_path)
            self.assertTrue(volume.get("read_only"), volume)
            self.assertFalse(
                volume.get("bind", {}).get("create_host_path"),
                volume,
            )

    def test_shadow_runner_has_no_weather_client(self) -> None:
        source = (ROOT / "pv-shadow-prediction" / "main.py").read_text(
            encoding="utf-8"
        ).lower()
        for forbidden in (
            "import urllib",
            "import requests",
            "import httpx",
            "open_meteo",
            "api.open-meteo.com",
        ):
            self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()
