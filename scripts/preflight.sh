#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
command -v docker >/dev/null || { echo "Install Docker Engine and Compose v2 (or enable Docker Desktop WSL integration)." >&2; exit 1; }
docker info >/dev/null
python3 scripts/bootstrap.py --check
memory_kb="$(awk '/MemTotal/ {print $2}' /proc/meminfo)"
if (( memory_kb < 12000000 )); then
  echo "Full stack targets >=16 GiB RAM (core >=12 GiB). This host has $((memory_kb / 1024)) MiB." >&2
  exit 1
fi
docker compose --env-file .env.deploy config --quiet
