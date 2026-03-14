#!/usr/bin/env bash
set -euo pipefail

source .env

echo "== MinIO (live) =="
curl -fsS "http://localhost:${MINIO_API_PORT}/minio/health/live" >/dev/null && echo "OK"

echo "== MinIO Console =="
curl -fsS "http://localhost:${MINIO_CONSOLE_PORT}/" >/dev/null && echo "OK"

echo "== Iceberg REST =="
curl -fsS "http://localhost:${ICEBERG_REST_PORT}/" >/dev/null && echo "OK"

echo "== Spark Master UI =="
curl -fsS "http://localhost:${SPARK_MASTER_UI_PORT}/" >/dev/null && echo "OK"

echo "== ClickHouse ping =="
curl -fsS "http://localhost:${CLICKHOUSE_HTTP_PORT}/ping" && echo

echo "== Superset health =="
curl -fsS "http://localhost:${SUPERSET_PORT}/health" && echo

echo "All services look up."