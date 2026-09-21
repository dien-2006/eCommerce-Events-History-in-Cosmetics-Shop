#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
.venv/bin/ruff check lakehouse airflow tests scripts superset
.venv/bin/python -m pytest -m 'not integration' -q
