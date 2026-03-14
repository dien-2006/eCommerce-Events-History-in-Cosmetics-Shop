# ClickHouse ↔ Iceberg on MinIO (Zero-copy vs Copy)

## Recommended for stability (student demo)
- Copy Gold Iceberg tables into ClickHouse MergeTree using Spark JDBC write.
- You get predictable performance and easy Superset connectivity.

## Optional zero-copy (read-only)
- ClickHouse supports reading Iceberg tables via `iceberg` table function / Iceberg engine (version-dependent).
- If you use it, pass correct S3 endpoint, path-style access, and credentials.

## Common troubleshooting
- 403 / signature mismatch → ensure path-style access, correct creds
- TLS issues → set ssl disabled for MinIO endpoint
- wrong location → verify actual Iceberg table location in MinIO bucket