#!/usr/bin/env python3
"""Generate a separate v2 environment; never overwrite existing secrets or raw data."""

import argparse
import base64
import os
import re
import secrets
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    target = ROOT / ".env.deploy"
    if not target.exists() and not args.check:
        contents = (ROOT / ".env.example").read_text()
        lines = []
        for line in contents.splitlines():
            if line.endswith("=GENERATE_ME"):
                name = line.split("=", 1)[0]
                value = (
                    base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()
                    if name == "AIRFLOW_FERNET_KEY"
                    else secrets.token_hex(24)
                )
                line = f"{name}={value}"
            if line.startswith("AIRFLOW_UID="):
                line = f"AIRFLOW_UID={os.getuid()}"
            lines.append(line)
        descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            stream.write("\n".join(lines) + "\n")
        print("Created .env.deploy with generated secrets (mode 0600).")
    if not target.exists():
        raise SystemExit("Missing .env.deploy; run python3 scripts/bootstrap.py")
    values = dict(
        line.split("=", 1)
        for line in target.read_text().splitlines()
        if line and not line.startswith("#") and "=" in line
    )
    if "GENERATE_ME" in values.values():
        raise SystemExit("Replace GENERATE_ME placeholders before deployment")
    for name in (
        "POSTGRES_PASSWORD",
        "AIRFLOW_DB_PASSWORD",
        "ICEBERG_DB_PASSWORD",
        "OPS_DB_PASSWORD",
        "SUPERSET_DB_PASSWORD",
    ):
        if not re.fullmatch(r"[a-zA-Z0-9_-]{20,}", values.get(name, "")):
            raise SystemExit(
                f"{name} must be >=20 URL-safe characters; do not put URL metacharacters in DSNs"
            )
    if not args.check:
        (ROOT / "data/rawdata").mkdir(parents=True, exist_ok=True)
        (ROOT / "artifacts").mkdir(exist_ok=True)
        profile = ROOT / "dbt/profiles.yml"
        if not profile.exists():
            profile.write_text((ROOT / "dbt/profiles.yml.example").read_text())
    print("Environment checks passed; original .env unchanged.")


if __name__ == "__main__":
    main()
