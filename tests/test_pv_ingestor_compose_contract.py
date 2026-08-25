"""Contract checks for the persistence-free PV ingestor previews."""

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

    def preview(self, compose):
        service = compose.get("services", {}).get(SERVICE)
        self.assertIsNotNone(service)
        return service

    def test_previews_require_the_same_explicit_profile(self):
        for compose in (self.local, self.production):
            self.assertEqual(
                ["pv-ingestor-preview"],
                self.preview(compose).get("profiles"),
            )

    def test_previews_have_no_database_or_firestore_integration(self):
        for compose in (self.local, self.production):
            service = self.preview(compose)
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

    def test_previews_expose_no_host_surface_and_are_read_only(self):
        for compose in (self.local, self.production):
            service = self.preview(compose)
            self.assertNotIn("ports", service)
            self.assertNotIn("container_name", service)
            self.assertTrue(service.get("read_only"))
            self.assertIn("ALL", service.get("cap_drop", []))
            self.assertIn("no-new-privileges:true", service.get("security_opt", []))

    def test_previews_require_explicit_live_cli_mode(self):
        for compose in (self.local, self.production):
            self.assertEqual(["--live"], self.preview(compose).get("command"))


if __name__ == "__main__":
    unittest.main()
