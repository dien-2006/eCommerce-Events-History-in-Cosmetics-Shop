#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
command -v java >/dev/null || { echo "Java 17 is required" >&2; exit 1; }
.venv/bin/pip install pyspark==3.5.7
bash scripts/download_jars.sh
python3 scripts/bootstrap.py
exec .venv/bin/python scripts/run_spark_tests.py
