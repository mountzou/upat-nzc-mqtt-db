#!/usr/bin/env python3
"""Read-only equivalence check for a staged VPS production Compose file.

Run on the production host. Output contains field paths and booleans, never
resolved environment values. Does not build, pull, start, stop or deploy services.
"""
import argparse
import json
import pathlib
import subprocess
import sys


def output(argv, cwd):
    return subprocess.check_output(argv, cwd=cwd, stderr=subprocess.PIPE)


def differences(left, right, path=""):
    if isinstance(left, dict) and isinstance(right, dict):
        return [item for key in sorted(left.keys() | right.keys())
                for item in ([path + "/" + key] if key not in left or key not in right
                             else differences(left[key], right[key], path + "/" + key))]
    return [] if left == right else [path]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=pathlib.Path, required=True)
    parser.add_argument("--project-directory", type=pathlib.Path,
                        default=pathlib.Path("/opt/upat-nzc-mqtt-db"))
    args = parser.parse_args()
    root = args.project_directory.resolve()
    candidate = args.candidate.resolve(strict=True)

    def run(argv):
        return output(argv, root)

    def inspect(name):
        return json.loads(run(["docker", "inspect", name]))[0]

    def containers():
        ids = run(["docker", "ps", "-aq"]).decode().split()
        return {x["Name"]: x for x in json.loads(run(["docker", "inspect", *ids]))}

    before = containers()
    api = before["/iot_api"]
    project = api["Config"]["Labels"]["com.docker.compose.project"]

    def source_files(container):
        return container["Config"]["Labels"]["com.docker.compose.project.config_files"].split(",")

    def compose(files, all_profiles=True):
        cmd = ["docker", "compose", "--project-directory", str(root),
               "--env-file", str(root / ".env"), "-p", project]
        if all_profiles:
            cmd += ["--profile", "*"]
        for path in files:
            cmd += ["-f", str(path)]
        return cmd

    def config(files, all_profiles=True):
        return json.loads(run(compose(files, all_profiles) + ["config", "--format", "json"]))

    files = source_files(api)
    watched = {pathlib.Path(path) for path in files} | {candidate, root / ".env"}
    for container in before.values():
        labels = container["Config"].get("Labels") or {}
        if labels.get("com.docker.compose.project") == project:
            watched.update(pathlib.Path(path) for path in source_files(container))
    snapshots = {path: path.read_bytes() for path in watched}
    expected = config(files)
    errors = []
    for name, service in expected["services"].items():
        container = before.get("/" + service.get("container_name", ""))
        if not container:
            continue  # Optional/uninstalled jobs retain the production source contract.
        if service.get("profiles") and not container["State"]["Running"]:
            # A stopped historical job container is not the scheduled deployment.
            # Compare its proposed configuration with the canonical production source.
            continue
        own_files = source_files(container)
        watched.update(pathlib.Path(path) for path in own_files)
        installed_contract = config(own_files)["services"][name]
        actual_env = dict(entry.split("=", 1) for entry in container["Config"]["Env"])
        for key, value in installed_contract.get("environment", {}).items():
            if actual_env.get(key) != value:
                errors.append("installed_environment/" + name + "/" + key)
        image = installed_contract.get("image")
        if image and inspect(image)["Id"] != container["Image"]:
            errors.append("installed_image/" + name)
        expected["services"][name] = installed_contract

    # Configuration values remain in memory; only comparison results are emitted.
    actual = config([candidate])
    errors += differences(expected, actual)
    default_expected = {**expected, "services": {
        k: v for k, v in expected["services"].items() if not v.get("profiles")}}
    errors += ["default_profile" + path for path in
               differences(default_expected, config([candidate], False))]
    hash_equal = (run(compose(files) + ["config", "--hash", "api"]) ==
                  run(compose([candidate]) + ["config", "--hash", "api"]))
    if not hash_equal:
        errors.append("api_compose_hash")
    after = containers()
    stable_fields = lambda x: (x["Id"], x["Image"], x["State"]["StartedAt"],
                              x["State"]["Status"], x["RestartCount"])
    stable = before.keys() == after.keys() and all(
        stable_fields(before[k]) == stable_fields(after[k]) for k in before)
    if not stable:
        errors.append("container_state_changed_during_check")
    if any(path.read_bytes() != data for path, data in snapshots.items()):
        errors.append("configuration_changed_during_check")
    print(json.dumps({"ok": not errors, "read_only": True,
                      "services_checked": len(expected["services"]),
                      "api_compose_hash_unchanged": hash_equal,
                      "containers_unchanged": stable,
                      "difference_paths": errors}, indent=2))
    return int(bool(errors))


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (subprocess.CalledProcessError, OSError, KeyError, ValueError) as exc:
        # Docker/Compose stderr may contain credentials. Never echo it.
        print(json.dumps({"ok": False, "error_type": type(exc).__name__,
                          "message": "Read-only verification failed; private details suppressed."}))
        sys.exit(1)
