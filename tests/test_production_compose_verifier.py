"""Exercise the production checker with synthetic Docker evidence only."""

import copy
import importlib.util
import io
import json
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "ops/verify-production-compose.py"
SPEC = importlib.util.spec_from_file_location("production_compose_verifier", SCRIPT)
verifier = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verifier)


class ProductionComposeVerifierTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        self.base = self.root / "base.yml"
        self.collector = self.root / "collector.yml"
        self.historical = self.root / "historical-job.yml"
        self.candidate = self.root / "candidate.yml"
        for path in (self.base, self.collector, self.historical, self.candidate):
            path.write_text("synthetic compose input\n")
        (self.root / ".env").write_text("TOKEN=fixture-private-value\n")
        self.expected = {"name": "test-project", "services": {
            "api": {"container_name": "iot_api", "image": "api-release",
                    "environment": {"TOKEN": "fixture-private-value"},
                    "ports": ["127.0.0.1:8000:8000"]},
            "collector": {"container_name": "collector", "image": "old-collector"},
            "energy-aggregator": {"container_name": "energy_aggregator",
                                  "image": "current-aggregator", "profiles": ["jobs"],
                                  "environment": {"SHELLY_COUNTER_START": "2026-09-07T08:00:00+00:00"}},
            "optional-job": {"profiles": ["jobs"], "image": "optional-release"},
        }, "volumes": {"postgres_data": {"name": "test-project_postgres_data"}}}
        own = copy.deepcopy(self.expected)
        own["services"]["collector"]["image"] = "installed-collector"
        self.proposed = copy.deepcopy(own)
        self.configs = {str(self.base): self.expected, str(self.collector): own}
        self.image_ids = {"api-release": "api-id", "installed-collector": "collector-id"}
        self.containers = [
            self.container("iot_api", "api-id", self.base, ["TOKEN=fixture-private-value"]),
            self.container("collector", "collector-id", self.collector),
            self.container("energy_aggregator", "historical-job-id", self.historical,
                           ["SHELLY_COUNTER_START=historical"], running=False),
        ]
        self.inspect_count = 0
        self.api_hash_matches = True
        self.wrong_default_profile = False
        self.change_container = False
        self.change_source = False

    def container(self, name, image, source, environment=(), running=True):
        return {"Name": "/" + name, "Id": name + "-container-id", "Image": image,
                "Config": {"Labels": {"com.docker.compose.project": "test-project",
                                      "com.docker.compose.project.config_files": str(source)},
                           "Env": list(environment)},
                "State": {"Running": running, "StartedAt": "2026-09-07T10:00:00Z",
                          "Status": "running" if running else "exited"},
                "RestartCount": 0}

    def docker_output(self, argv, cwd):
        self.assertEqual(self.root, cwd)
        if argv == ["docker", "ps", "-aq"]:
            return b"fixture-containers"
        if argv == ["docker", "inspect", "fixture-containers"]:
            self.inspect_count += 1
            containers = copy.deepcopy(self.containers)
            if self.inspect_count == 2:
                if self.change_container:
                    containers[0]["RestartCount"] += 1
                if self.change_source:
                    (self.root / ".env").write_text("TOKEN=changed-private-value\n")
            return json.dumps(containers).encode()
        if argv[:2] == ["docker", "inspect"]:
            return json.dumps([{"Id": self.image_ids[argv[2]]}]).encode()
        self.assertEqual(["docker", "compose"], argv[:2])
        self.assertIn("config", argv)  # No execution/build/pull command is permitted.
        self.assertEqual(str(self.root / ".env"), argv[argv.index("--env-file") + 1])
        self.assertEqual("test-project", argv[argv.index("-p") + 1])
        source = argv[argv.index("-f") + 1]
        if "--hash" in argv:
            self.assertEqual("api", argv[-1])
            return b"same-hash" if self.api_hash_matches or source != str(self.candidate) else b"different-hash"
        config = copy.deepcopy(self.proposed if source == str(self.candidate) else self.configs[source])
        if "--profile" not in argv:
            config["services"] = {k: v for k, v in config["services"].items() if not v.get("profiles")}
            if self.wrong_default_profile:
                config["services"]["optional-job"] = self.proposed["services"]["optional-job"]
        return json.dumps(config).encode()

    def check(self):
        stream = io.StringIO()
        argv = [str(SCRIPT), "--candidate", str(self.candidate), "--project-directory", str(self.root)]
        with patch.object(sys, "argv", argv), patch.object(verifier, "output", side_effect=self.docker_output), redirect_stdout(stream):
            code = verifier.main()
        self.assertNotIn("private-value", stream.getvalue())
        result = json.loads(stream.getvalue())
        self.assertEqual(int(not result["ok"]), code)
        return result

    def test_equivalent_contract_uses_own_collector_source_and_ignores_stopped_profiled_job(self):
        result = self.check()
        self.assertTrue(result["ok"])
        self.assertEqual(4, result["services_checked"])
        self.assertEqual([], result["difference_paths"])

    def test_unrelated_container_without_labels_does_not_block_verification(self):
        unrelated = self.container("unrelated", "other-id", self.base)
        unrelated["Config"]["Labels"] = None
        self.containers.append(unrelated)
        self.assertTrue(self.check()["ok"])

    def test_wrong_cutover_is_rejected(self):
        self.proposed["services"]["energy-aggregator"]["environment"]["SHELLY_COUNTER_START"] = "wrong"
        self.assertIn("/services/energy-aggregator/environment/SHELLY_COUNTER_START", self.check()["difference_paths"])

    def test_wrong_api_port_is_rejected(self):
        self.proposed["services"]["api"]["ports"] = ["8000:8000"]
        self.assertIn("/services/api/ports", self.check()["difference_paths"])

    def test_wrong_candidate_image_is_rejected(self):
        self.proposed["services"]["collector"]["image"] = "unreviewed-image"
        self.assertIn("/services/collector/image", self.check()["difference_paths"])

    def test_installed_image_drift_is_rejected(self):
        self.image_ids["api-release"] = "unexpected-id"
        self.assertIn("installed_image/api", self.check()["difference_paths"])

    def test_installed_environment_drift_is_rejected_without_values(self):
        self.containers[0]["Config"]["Env"] = ["TOKEN=changed-private-value"]
        self.assertIn("installed_environment/api/TOKEN", self.check()["difference_paths"])

    def test_changed_api_hash_is_rejected(self):
        self.api_hash_matches = False
        self.assertIn("api_compose_hash", self.check()["difference_paths"])

    def test_changed_container_during_check_is_rejected(self):
        self.change_container = True
        self.assertIn("container_state_changed_during_check", self.check()["difference_paths"])

    def test_changed_source_during_check_is_rejected(self):
        self.change_source = True
        self.assertIn("configuration_changed_during_check", self.check()["difference_paths"])

    def test_profiled_job_in_default_selection_is_rejected(self):
        self.wrong_default_profile = True
        self.assertIn("default_profile/services/optional-job", self.check()["difference_paths"])

    def test_cli_suppresses_private_docker_error_details(self):
        argv = [str(SCRIPT), "--candidate", str(self.candidate), "--project-directory", str(self.root)]
        failure = subprocess.CalledProcessError(1, "docker", stderr=b"fixture-private-value")
        stream = io.StringIO()
        with patch.object(sys, "argv", argv), patch("subprocess.check_output", side_effect=failure), redirect_stdout(stream):
            with self.assertRaises(SystemExit) as raised:
                runpy.run_path(str(SCRIPT), run_name="__main__")
        self.assertEqual(1, raised.exception.code)
        self.assertNotIn("private-value", stream.getvalue())
        self.assertFalse(json.loads(stream.getvalue())["ok"])


if __name__ == "__main__":
    unittest.main()
