#!/usr/bin/env bash
set -euo pipefail
# Separate roles/databases even though the single-host deployment shares one server.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=airflow_password="$AIRFLOW_DB_PASSWORD" \
  --set=iceberg_password="$ICEBERG_DB_PASSWORD" \
  --set=ops_password="$OPS_DB_PASSWORD" \
  --set=superset_password="$SUPERSET_DB_PASSWORD" <<'SQL'
CREATE USER airflow WITH PASSWORD :'airflow_password';
CREATE DATABASE airflow OWNER airflow;
CREATE USER iceberg WITH PASSWORD :'iceberg_password';
CREATE DATABASE iceberg OWNER iceberg;
CREATE USER pipeline WITH PASSWORD :'ops_password';
CREATE DATABASE operations OWNER pipeline;
CREATE USER superset WITH PASSWORD :'superset_password';
CREATE DATABASE superset OWNER superset;
SQL
