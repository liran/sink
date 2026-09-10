#!/usr/bin/env python3
"""Run a reviewed JSON list of sequential capacity scenarios; resume saved runs."""

import argparse
import json
import pathlib
import re
import subprocess
import sys

import cluster


def flags(values):
    result = []
    for key, value in values.items():
        if isinstance(value, bool):
            result.append(f"--{key}={str(value).lower()}")
        else:
            result.extend([f"--{key}", str(value)])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--output-dir", required=True)
    opts = parser.parse_args()
    state = cluster.owned_state(opts)
    plan = json.loads(pathlib.Path(opts.plan).read_text())
    root = pathlib.Path(__file__).resolve().parent
    previous_server = None
    for scenario in plan:
        label = scenario["label"]
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,100}", label):
            raise RuntimeError("Use simple lowercase labels, without paths")
        destination = cluster.private_artifact_path(opts.output_dir) / (label + ".json")
        if destination.exists():
            saved = json.loads(destination.read_text())
            if saved.get("verified") and saved.get("returncode") == 0 and not saved.get("harness_error"):
                print(f"Already measured: {label}", flush=True)
                continue
        server = scenario.get("server") or {}
        if server != previous_server:
            command = [sys.executable, str(root / "cluster.py"), "--state", opts.state, "server", *flags(server)]
            subprocess.run(command, check=True)
            command = ["kubectl", "--context", cluster.BOUND_CONTEXT, "-n", state["namespace"], "rollout", "status", "deployment/sink", "--timeout=300s"]
            subprocess.run(command, check=True)
            previous_server = server
        print(f"Measuring: {label}", flush=True)
        command = [sys.executable, str(root / "run.py"), "--state", opts.state, "--output", str(destination), "--label", label]
        if scenario.get("fault"):
            command.extend(["--fault", scenario["fault"], "--fault-after-seconds", str(scenario.get("fault_after_seconds", 20))])
        command.extend(["--", *flags(scenario["load"])])
        subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
