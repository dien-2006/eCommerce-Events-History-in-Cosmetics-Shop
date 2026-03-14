#!/bin/sh
set -eu

echo "[minio-init] Waiting for MinIO..."
until /usr/bin/mc alias set "${MINIO_ALIAS}" "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" --api S3v4 >/dev/null 2>&1; do
  sleep 1
done

echo "[minio-init] Creating bucket if missing: ${MINIO_BUCKET}"
/usr/bin/mc mb --ignore-existing "${MINIO_ALIAS}/${MINIO_BUCKET}"

echo "[minio-init] Enabling versioning (recommended for Iceberg demos)"
/usr/bin/mc version enable "${MINIO_ALIAS}/${MINIO_BUCKET}" || true

echo "[minio-init] Done."