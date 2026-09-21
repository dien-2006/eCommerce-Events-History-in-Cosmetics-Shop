"""Extract and load exact source bytes before any business transformation."""

import csv
import hashlib
import io
import json
import os
import tempfile
from pathlib import Path

from botocore.exceptions import ClientError

from lakehouse.config import validate_header


def spool_source(path: Path):
    """Snapshot bytes once; hashing and uploading the same snapshot avoids a TOCTOU race."""
    spool = tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024, mode="w+b")
    sha = hashlib.sha256()
    size = 0
    try:
        before = path.stat()
        with path.open("rb") as stream:
            while chunk := stream.read(4 * 1024 * 1024):
                sha.update(chunk)
                size += len(chunk)
                spool.write(chunk)
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise RuntimeError("Source changed during landing; submit only completed files")
        spool.seek(0)
        return spool, sha.hexdigest(), size
    except BaseException:
        spool.close()
        raise


def read_header(stream):
    wrapper = io.TextIOWrapper(stream, encoding="utf-8-sig", newline="")
    try:
        header = next(csv.reader(wrapper), [])
    finally:
        wrapper.detach()
        stream.seek(0)
    return validate_header(header)


def land(project, registry, key, s3):
    if registry.run(key)["planned"]:
        return {"files": len(registry.files(key)), "manifest_reused": True}
    if not project.input_dir.is_dir():
        raise FileNotFoundError(f"Input directory not found: {project.input_dir}")
    bucket = os.environ["S3_BUCKET"]
    for path in sorted(project.input_dir.rglob("*.csv")):
        if path.is_symlink() or not path.resolve().is_relative_to(project.input_dir.resolve()):
            raise ValueError("Input symlinks are not supported")
        spool, checksum, size = spool_source(path)
        with spool:
            object_key = f"raw/{project.name}/sha256={checksum}/events.csv"
            known = registry.db.execute(
                "SELECT 1 FROM ops.files WHERE project=%s AND sha256=%s",
                (project.name, checksum),
            ).fetchone()
            if known:
                continue
            header_error = None
            try:
                header = read_header(spool)
            except (ValueError, UnicodeError, csv.Error) as exc:
                header_error = exc
            # A retry after raw upload must not create redundant object versions.
            try:
                head = s3.head_object(Bucket=bucket, Key=object_key)
            except ClientError as exc:
                if exc.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
                    raise
                # Retain raw bytes even when the header contract fails below.
                s3.upload_fileobj(
                    spool,
                    bucket,
                    object_key,
                    ExtraArgs={
                        "Metadata": {"sha256": checksum},
                        "ContentType": "text/csv",
                    },
                )
                head = s3.head_object(Bucket=bucket, Key=object_key)
            if head["ContentLength"] != size or head["Metadata"].get("sha256") != checksum:
                raise RuntimeError("Raw object integrity check failed")
            if header_error:
                raise header_error
        registry.db.execute(
            "INSERT INTO ops.files(project, sha256, object_key, original_name, size_bytes, header) "
            "VALUES (%s,%s,%s,%s,%s,%s::jsonb) ON CONFLICT DO NOTHING",
            (project.name, checksum, object_key, path.name, size, json.dumps(header)),
        )
    with registry.db.transaction():
        registry.db.execute(
            "INSERT INTO ops.run_files(run_key, project, sha256) "
            "SELECT %s, project, sha256 FROM ops.files WHERE project=%s "
            "AND bronze_loaded_at IS NULL AND retired_at IS NULL "
            "ON CONFLICT DO NOTHING",
            (key, project.name),
        )
        registry.db.execute("UPDATE ops.runs SET planned=true WHERE run_key=%s", (key,))
    return {"files": len(registry.files(key)), "manifest_reused": False}
