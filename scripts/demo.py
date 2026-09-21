#!/usr/bin/env python3
"""Run the full local acceptance scenario via Airflow, then enable BI and monitoring."""

import json
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "--env-file", ".env.deploy"]


def run(*args, **kwargs):
    return subprocess.run(list(args), cwd=ROOT, check=True, **kwargs)


def main():
    run("python3", "scripts/bootstrap.py")
    run("bash", "scripts/preflight.sh")
    run(*COMPOSE, "up", "-d", "--build", "--wait", "--wait-timeout", "600")
    run_id = f"acceptance_{int(time.time())}"
    run(*COMPOSE, "exec", "-T", "airflow-scheduler", "airflow", "dags", "unpause", "cosmetics_elt")
    run(
        *COMPOSE,
        "exec",
        "-T",
        "airflow-scheduler",
        "airflow",
        "dags",
        "trigger",
        "--run-id",
        run_id,
        "cosmetics_elt",
    )
    deadline = time.monotonic() + 4 * 3600
    while time.monotonic() < deadline:
        result = run(
            *COMPOSE,
            "exec",
            "-T",
            "airflow-scheduler",
            "/opt/pipeline-venv/bin/python",
            "-m",
            "lakehouse.cli",
            "status",
            capture_output=True,
            text=True,
        )
        matches = [r for r in json.loads(result.stdout) if r["orchestrator_run_id"] == run_id]
        if matches and matches[0]["status"] == "success":
            print(json.dumps(matches[0], indent=2))
            break
        # A failed stage may still have Airflow retries pending; keep polling.
        print(f"Waiting for {run_id}: {matches[0]['status'] if matches else 'queued'}", flush=True)
        time.sleep(30)
    else:
        raise SystemExit(
            "Acceptance timed out. Inspect Airflow logs; no successful deployment claimed."
        )
    run(
        *COMPOSE,
        "--profile",
        "bi",
        "--profile",
        "observability",
        "up",
        "-d",
        "--build",
        "--wait",
        "--wait-timeout",
        "600",
    )
    print("ELT completed. Airflow :8080 · Superset :8088 · Grafana :3000")


if __name__ == "__main__":
    main()
