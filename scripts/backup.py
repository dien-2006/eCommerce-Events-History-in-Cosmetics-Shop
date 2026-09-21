#!/usr/bin/env python3
"""Cold volume backups with checksums; restore only into new, empty Compose volumes."""

import argparse
import hashlib
import json
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def call(args, **kwargs):
    return subprocess.run(args, cwd=ROOT, check=True, **kwargs)


def capture(args):
    return call(args, capture_output=True, text=True).stdout


def checksum(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["create", "restore"])
    parser.add_argument("--env-file", default=".env.deploy")
    parser.add_argument(
        "--stop-services", action="store_true", help="Acknowledge downtime for cold backup"
    )
    parser.add_argument("--backup", type=Path)
    parser.add_argument("--target-project")
    args = parser.parse_args()
    compose = [
        "docker",
        "compose",
        "--env-file",
        args.env_file,
        "--profile",
        "bi",
        "--profile",
        "observability",
    ]
    if args.action == "restore":
        if not args.backup or not args.target_project:
            parser.error("restore needs --backup and a new --target-project")
        if not re.fullmatch(r"[a-z][a-z0-9_-]{1,50}", args.target_project):
            parser.error("Invalid target project name")
        compose += ["-p", args.target_project]
    config = json.loads(capture([*compose, "config", "--format", "json"]))
    if args.action == "create":
        if not args.stop_services:
            parser.error(
                "Pause DAGs and wait for runs to finish, then pass --stop-services for a cold backup"
            )
        check = "from lakehouse.connections import postgres; c=postgres(); print(c.execute(\"SELECT count(*) AS n FROM ops.runs WHERE status='running'\").fetchone()['n'])"
        active = capture(
            [
                *compose,
                "exec",
                "-T",
                "airflow-scheduler",
                "/opt/pipeline-venv/bin/python",
                "-c",
                check,
            ]
        ).strip()
        if active != "0":
            raise SystemExit("There are active pipeline runs; wait before taking a cold backup")
        running = capture([*compose, "ps", "--status", "running", "--services"]).split()
        destination = args.backup or ROOT / "backups" / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        destination = destination.resolve()
        destination.mkdir(parents=True, exist_ok=False)
        manifest = {
            "project": config["name"],
            "created_at": datetime.now(UTC).isoformat(),
            "images": {k: v["image"] for k, v in config["services"].items()},
            "volumes": {},
        }
        call([*compose, "stop", "--timeout", "120"])
        try:
            existing = set(
                capture(["docker", "volume", "ls", "--format", "{{.Name}}"]).splitlines()
            )
            for logical, specification in config["volumes"].items():
                volume = specification["name"]
                if volume not in existing:
                    continue
                archive = logical + ".tar.gz"
                call(
                    [
                        "docker",
                        "run",
                        "--rm",
                        "-v",
                        f"{volume}:/source:ro",
                        "-v",
                        f"{destination}:/backup",
                        "alpine:3.22.1",
                        "tar",
                        "-czf",
                        f"/backup/{archive}",
                        "-C",
                        "/source",
                        ".",
                    ]
                )
                manifest["volumes"][logical] = {
                    "archive": archive,
                    "sha256": checksum(destination / archive),
                }
            (destination / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        finally:
            if running:
                call([*compose, "start", *running])
        print(f"Cold backup: {destination}. Store .env.deploy separately in encrypted storage.")
    else:
        source = args.backup.resolve()
        manifest = json.loads((source / "manifest.json").read_text())
        if manifest["project"] == args.target_project:
            raise SystemExit(
                "Restore into a NEW project; refusing to overwrite the source deployment"
            )
        existing = set(capture(["docker", "volume", "ls", "--format", "{{.Name}}"]).splitlines())
        for logical, archive in manifest["volumes"].items():
            if logical not in config["volumes"] or config["volumes"][logical]["name"] in existing:
                raise SystemExit(f"Unknown or existing target volume: {logical}")
            expected_name = logical + ".tar.gz"
            if (
                archive["archive"] != expected_name
                or checksum(source / expected_name) != archive["sha256"]
            ):
                raise SystemExit(f"Backup integrity check failed: {logical}")
        for logical, archive in manifest["volumes"].items():
            volume = config["volumes"][logical]["name"]
            call(
                [
                    "docker",
                    "volume",
                    "create",
                    "--label",
                    f"com.docker.compose.project={args.target_project}",
                    "--label",
                    f"com.docker.compose.volume={logical}",
                    volume,
                ]
            )
            call(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-v",
                    f"{volume}:/target",
                    "-v",
                    f"{source}:/backup:ro",
                    "alpine:3.22.1",
                    "tar",
                    "-xzf",
                    f"/backup/{archive['archive']}",
                    "-C",
                    "/target",
                ]
            )
        print(
            "Volumes restored. Set non-conflicting ports and use the backup's secrets/image versions before starting."
        )


if __name__ == "__main__":
    main()
