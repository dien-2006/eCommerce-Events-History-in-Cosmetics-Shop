#!/usr/bin/env python3
"""Fail status checks when required or enabled Compose services are not ready."""

import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "--env-file", ".env.deploy"]


def parse_containers(output):
    if not output.strip():
        return []
    if output.lstrip().startswith("["):
        return json.loads(output)
    return [json.loads(line) for line in output.splitlines() if line.strip()]


def readiness_errors(services, containers):
    grouped = {}
    for container in containers:
        grouped.setdefault(container["Service"], []).append(container)
    errors = []
    for name, spec in services.items():
        if spec.get("profiles") and name not in grouped:
            continue  # Optional services need checking once their containers exist.
        instances = grouped.get(name, [])
        if not instances:
            errors.append(f"{name}: missing container")
        for container in instances:
            state = container.get("State", "unknown")
            if name.endswith("-init"):
                if state != "exited" or container.get("ExitCode") != 0:
                    errors.append(f"{name}: init has not completed successfully ({state})")
                continue
            if state != "running":
                errors.append(f"{name}: {state}")
            elif spec.get("healthcheck") and not spec["healthcheck"].get("disable"):
                health = container.get("Health", "missing")
                if health != "healthy":
                    errors.append(f"{name}: health={health}")
    return errors


def main():
    # Compose config includes credentials; capture it and never print it.
    config = json.loads(
        subprocess.check_output(
            [*COMPOSE, "--profile", "*", "config", "--format", "json"], cwd=ROOT, text=True
        )
    )
    containers = parse_containers(
        subprocess.check_output([*COMPOSE, "ps", "--all", "--format", "json"], cwd=ROOT, text=True)
    )
    errors = readiness_errors(config["services"], containers)
    if errors:
        raise SystemExit("Services are not ready:\n" + "\n".join(f"- {error}" for error in errors))
    print("Required and existing optional services are ready.")


if __name__ == "__main__":
    main()
