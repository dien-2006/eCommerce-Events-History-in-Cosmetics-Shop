#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose --env-file .env.deploy ps
python3 scripts/check_services.py
docker compose --env-file .env.deploy exec -T airflow-scheduler python -c '
import json, subprocess
from lakehouse.config import load_projects
errors = json.loads(subprocess.check_output(["airflow", "dags", "list-import-errors", "--output", "json"]))
print(json.dumps(errors, indent=2))
if errors:
    raise SystemExit("Airflow has DAG import errors")
dags = json.loads(subprocess.check_output(["airflow", "dags", "list", "--output", "json"]))
missing = {f"{name}_elt" for name in load_projects()} - {dag["dag_id"] for dag in dags}
if missing:
    raise SystemExit(f"Missing project DAGs: {sorted(missing)}")
'
docker compose --env-file .env.deploy exec -T airflow-scheduler \
  /opt/pipeline-venv/bin/python -m lakehouse.cli status
