"""Deployment contracts for the fail-closed internal authentication boundary."""

from __future__ import annotations

import json
import subprocess
import unittest
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOCAL_COMPOSE = ROOT / "docker-compose.yml"
PRODUCTION_COMPOSE = ROOT / "docker-compose.prod.yml"


def resolved_compose(path: Path) -> dict[str, Any]:
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
        raise unittest.SkipTest("Docker Compose is required") from exc
    if completed.returncode != 0:
        raise AssertionError(completed.stderr.strip())
    return json.loads(completed.stdout)


class AuthServiceDeploymentContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.local = resolved_compose(LOCAL_COMPOSE)
        cls.production = resolved_compose(PRODUCTION_COMPOSE)

    def test_api_receives_a_dedicated_auth_service_token(self):
        for compose in (self.local, self.production):
            environment = compose["services"]["api"]["environment"]
            self.assertIn("AUTH_SERVICE_TOKEN", environment)
            self.assertIn("OPS_TELEMETRY_TOKEN", environment)
            self.assertNotEqual(
                environment["AUTH_SERVICE_TOKEN"],
                environment["OPS_TELEMETRY_TOKEN"],
            )

    def test_production_postgres_remains_unpublished(self):
        self.assertNotIn("ports", self.production["services"]["postgres"])

    def test_caddy_exposes_only_the_narrow_internal_auth_prefix(self):
        caddyfile = (ROOT / "caddy" / "Caddyfile").read_text(encoding="utf-8")
        self.assertIn("handle /internal/auth/*", caddyfile)
        self.assertNotIn("handle /internal/*", caddyfile)

    def test_api_image_packages_the_auth_service_module(self):
        dockerfile = (ROOT / "api" / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn("COPY auth_service.py .", dockerfile)

    def test_example_environment_declares_no_auth_token_value(self):
        env_lines = (ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
        self.assertIn("AUTH_SERVICE_TOKEN=", env_lines)


if __name__ == "__main__":
    unittest.main()
