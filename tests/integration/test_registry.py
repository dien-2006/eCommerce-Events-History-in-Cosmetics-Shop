"""Exercise actual PostgreSQL checkpoints, immutable manifests and advisory locks."""

import os
import uuid
from types import SimpleNamespace

import psycopg
import pytest
from botocore.exceptions import ClientError
from psycopg.rows import dict_row

from lakehouse.config import RAW_COLUMNS
from lakehouse.landing import land
from lakehouse.registry import Registry

pytestmark = pytest.mark.integration


class MemoryObjects:
    def __init__(self):
        self.objects = {}
        self.uploads = 0

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return self.objects[Key]

    def upload_fileobj(self, stream, bucket, key, ExtraArgs):
        self.objects[key] = {"ContentLength": len(stream.read()), "Metadata": ExtraArgs["Metadata"]}
        self.uploads += 1
        stream.close()  # boto3's file transfer may close the supplied file object.


def test_postgres_manifest_retry_lock_and_failure(tmp_path, monkeypatch):
    url = os.environ.get("OPS_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set OPS_TEST_DATABASE_URL to an isolated PostgreSQL test database")
    monkeypatch.setenv("S3_BUCKET", "test")
    name = "qa_" + uuid.uuid4().hex[:8]
    project = SimpleNamespace(name=name, input_dir=tmp_path)
    (tmp_path / "first.csv").write_text(
        ",".join(RAW_COLUMNS) + "\n2020-01-01 UTC,purchase,1,1,c,b,10,1,s\n"
    )
    storage = MemoryObjects()
    with psycopg.connect(url, autocommit=True, row_factory=dict_row) as connection:
        registry = Registry(connection)
        registry.initialize()
        with registry.lock(name):
            with psycopg.connect(url, autocommit=True, row_factory=dict_row) as other:
                with pytest.raises(RuntimeError, match="active stage"), Registry(other).lock(name):
                    pass
            key = registry.begin(name, "first", {"as_of_date": None})
            with registry.stage(key, "land") as metrics:
                metrics.update(land(project, registry, key, storage))
            assert len(registry.files(key)) == 1
            assert storage.uploads == 1
            # New file does not alter an already frozen run manifest.
            (tmp_path / "second.csv").write_text(
                (tmp_path / "first.csv").read_text() + "2020-01-02 UTC,purchase,2,1,c,b,20,2,s2\n"
            )
            assert land(project, registry, key, storage)["manifest_reused"]
            assert storage.uploads == 1
            with pytest.raises(ValueError, match="immutable"):
                registry.begin(name, "first", {"as_of_date": "2020-02-01"})
            with pytest.raises(RuntimeError, match="simulated"), registry.stage(key, "bronze"):
                raise RuntimeError("simulated")
            assert registry.run(key)["status"] == "failed"
            with registry.stage(key, "bronze") as metrics:
                metrics["loaded_rows"] = 1
                connection.execute(
                    "UPDATE ops.files SET bronze_loaded_at=now() WHERE project=%s", (name,)
                )
            registry.require_stage(key, "bronze")
            second_key = registry.begin(name, "second", {"as_of_date": None})
            with registry.stage(second_key, "land") as metrics:
                metrics.update(land(project, registry, second_key, storage))
            assert len(registry.files(second_key)) == 1
            assert storage.uploads == 2
            with pytest.raises(RuntimeError, match="newer run"):
                registry.assert_current(key)
            sha = registry.files(key)[0]["sha256"]
            with pytest.raises(RuntimeError, match="active runs"):
                registry.retire_file(name, sha, "Source replacement approved", apply=True)
            connection.execute("UPDATE ops.runs SET status='failed' WHERE project=%s", (name,))
            registry.retire_file(name, sha, "Source replacement approved", apply=True)
            row = connection.execute(
                "SELECT * FROM ops.files WHERE project=%s AND sha256=%s", (name, sha)
            ).fetchone()
            assert row["retired_at"] is not None and row["needs_rebuild"]
            assert row["retirement_reason"] == "Source replacement approved"


def test_raw_survives_bad_header_without_uploading_again(tmp_path, monkeypatch):
    url = os.environ.get("OPS_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set OPS_TEST_DATABASE_URL to an isolated PostgreSQL test database")
    monkeypatch.setenv("S3_BUCKET", "test")
    name = "qa_" + uuid.uuid4().hex[:8]
    project = SimpleNamespace(name=name, input_dir=tmp_path)
    (tmp_path / "bad.csv").write_text("unexpected_header\nraw-value\n")
    storage = MemoryObjects()
    with psycopg.connect(url, autocommit=True, row_factory=dict_row) as connection:
        registry = Registry(connection)
        registry.initialize()
        key = registry.begin(name, "bad", {})
        for _ in range(2):
            with pytest.raises(ValueError, match="contract mismatch"):
                land(project, registry, key, storage)
        assert storage.uploads == 1
        assert not registry.files(key)
