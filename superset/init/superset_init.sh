#!/usr/bin/env bash
set -euo pipefail

export FLASK_APP="superset"

# Wait for DB
echo "[superset] Waiting for metadata DB..."
python - <<'PY'
import os, time
import psycopg2

cfg = dict(
  dbname=os.environ["DATABASE_DB"],
  user=os.environ["DATABASE_USER"],
  password=os.environ["DATABASE_PASSWORD"],
  host=os.environ["DATABASE_HOST"],
  port=int(os.environ["DATABASE_PORT"]),
)
for i in range(60):
  try:
    psycopg2.connect(**cfg).close()
    print("DB ready")
    break
  except Exception:
    time.sleep(1)
else:
  raise SystemExit("DB not ready")
PY

echo "[superset] Upgrading DB..."
superset db upgrade

echo "[superset] Creating admin (idempotent)..."
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@example.com \
  --password admin \
  || true

echo "[superset] Initializing roles/permissions..."
superset init

echo "[superset] Starting server..."
superset run -h 0.0.0.0 -p 8088