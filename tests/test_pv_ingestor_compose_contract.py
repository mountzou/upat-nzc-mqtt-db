"""Contract checks for the persistence-free local PV ingestor preview."""

from __future__ import annotations

import json
import subprocess
import unittest
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOCAL_COMPOSE = ROOT / "docker-compose.yml"
PRODUCTION_COMPOSE = ROOT / "docker-compose.prod.yml"
SERVICE = "pv-ingestor"


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


class PvIngestorComposeContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.local = resolved_compose(LOCAL_COMPOSE)
        cls.production = resolved_compose(PRODUCTION_COMPOSE)

    def preview(self):
        service = self.local.get("services", {}).get(SERVICE)
        self.assertIsNotNone(service)
        return service

    def test_preview_is_local_only_and_requires_explicit_profile(self):
        self.assertNotIn(SERVICE, self.production.get("services", {}))
        self.assertEqual(
            ["pv-ingestor-preview"],
            self.preview().get("profiles"),
        )

    def test_preview_has_no_database_or_firestore_integration(self):
        service = self.preview()
        rendered = json.dumps(service, sort_keys=True).lower()
        for forbidden in (
            "postgres_",
            "firebase",
            "firestore",
            "google_application_credentials",
        ):
            self.assertNotIn(forbidden, rendered)
        self.assertNotIn("depends_on", service)
        self.assertNotIn("volumes", service)

    def test_preview_exposes_no_host_surface_and_is_read_only(self):
        service = self.preview()
        self.assertNotIn("ports", service)
        self.assertNotIn("container_name", service)
        self.assertTrue(service.get("read_only"))
        self.assertIn("ALL", service.get("cap_drop", []))
        self.assertIn("no-new-privileges:true", service.get("security_opt", []))

    def test_preview_requires_explicit_live_cli_mode(self):
        self.assertEqual(["--live"], self.preview().get("command"))


if __name__ == "__main__":
    unittest.main()
