"""PostgreSQL control plane: immutable run manifests, checkpoints and stage history."""

import hashlib
import json
import time
from contextlib import contextmanager

DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.runs (
  run_key text PRIMARY KEY,
  sequence_id bigserial UNIQUE,
  project text NOT NULL,
  orchestrator_run_id text NOT NULL,
  status text NOT NULL DEFAULT 'running',
  options jsonb NOT NULL DEFAULT '{}',
  planned boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  metrics jsonb NOT NULL DEFAULT '{}',
  UNIQUE(project, orchestrator_run_id)
);
CREATE TABLE IF NOT EXISTS ops.files (
  project text NOT NULL,
  sha256 text NOT NULL,
  object_key text NOT NULL,
  original_name text NOT NULL,
  size_bytes bigint NOT NULL,
  header jsonb NOT NULL,
  landed_at timestamptz NOT NULL DEFAULT now(),
  bronze_loaded_at timestamptz,
  row_count bigint,
  PRIMARY KEY(project, sha256)
);
CREATE TABLE IF NOT EXISTS ops.run_files (
  run_key text REFERENCES ops.runs(run_key),
  project text NOT NULL,
  sha256 text NOT NULL,
  PRIMARY KEY(run_key, sha256),
  FOREIGN KEY(project, sha256) REFERENCES ops.files(project, sha256)
);
CREATE TABLE IF NOT EXISTS ops.stages (
  id bigserial PRIMARY KEY,
  run_key text REFERENCES ops.runs(run_key),
  stage text NOT NULL,
  status text NOT NULL,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  duration_seconds double precision,
  metrics jsonb NOT NULL DEFAULT '{}',
  error_type text
);
CREATE INDEX IF NOT EXISTS stages_run ON ops.stages(run_key, id);
CREATE INDEX IF NOT EXISTS runs_project_created ON ops.runs(project, created_at DESC);
ALTER TABLE ops.files ADD COLUMN IF NOT EXISTS retired_at timestamptz;
ALTER TABLE ops.files ADD COLUMN IF NOT EXISTS retirement_reason text;
ALTER TABLE ops.files ADD COLUMN IF NOT EXISTS needs_rebuild boolean NOT NULL DEFAULT false;
"""


def run_key(project: str, orchestrator_id: str) -> str:
    return hashlib.sha256(f"{project}\0{orchestrator_id}".encode()).hexdigest()


class Registry:
    def __init__(self, connection):
        self.db = connection

    def initialize(self):
        # Metrics and the first DAG may bootstrap together on a fresh deployment.
        with self.db.transaction():
            self.db.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended('lakehouse:ops-schema-v2', 0))"
            )
            self.db.execute(DDL)

    @contextmanager
    def lock(self, project):
        # Shared by Airflow and CLI. Prevent overlapping writes to one project.
        acquired = self.db.execute(
            "SELECT pg_try_advisory_lock(hashtextextended(%s, 0)) AS locked", (project,)
        ).fetchone()["locked"]
        if not acquired:
            raise RuntimeError(f"Project {project} has another active stage; retry later")
        try:
            yield
        finally:
            self.db.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (project,))

    def begin(self, project, orchestrator_id, options):
        key = run_key(project, orchestrator_id)
        self.db.execute(
            "INSERT INTO ops.runs(run_key, project, orchestrator_run_id, options) "
            "VALUES (%s,%s,%s,%s::jsonb) ON CONFLICT DO NOTHING",
            (key, project, orchestrator_id, json.dumps(options)),
        )
        run = self.run(key)
        if run["options"] != options:
            raise ValueError("A run's options are immutable. Use a new run ID to change options.")
        return key

    def run(self, key):
        row = self.db.execute("SELECT * FROM ops.runs WHERE run_key=%s", (key,)).fetchone()
        if not row:
            raise ValueError("Unknown run; execute land first")
        return row

    def files(self, key):
        return self.db.execute(
            "SELECT f.* FROM ops.files f JOIN ops.run_files r "
            "ON f.project=r.project AND f.sha256=r.sha256 WHERE r.run_key=%s ORDER BY f.sha256",
            (key,),
        ).fetchall()

    def assert_current(self, key):
        row = self.db.execute(
            "SELECT r.run_key FROM ops.runs r JOIN ops.runs mine ON mine.run_key=%s "
            "WHERE r.project=mine.project AND r.sequence_id>mine.sequence_id LIMIT 1",
            (key,),
        ).fetchone()
        if row:
            raise RuntimeError("A newer run exists. Replay with a new run ID, not a stale task.")

    def require_stage(self, key, stage):
        result = self.db.execute(
            "SELECT status FROM ops.stages WHERE run_key=%s AND stage=%s ORDER BY id DESC LIMIT 1",
            (key, stage),
        ).fetchone()
        if not result or result["status"] != "success":
            raise RuntimeError(f"Stage {stage} must succeed before continuing")

    def record_candidate(self, key, tables):
        self.db.execute(
            "UPDATE ops.runs SET metrics=metrics || %s::jsonb WHERE run_key=%s",
            (json.dumps({"candidate": {"tables": tables}}), key),
        )

    def retire_file(self, project, sha, reason, apply=False):
        row = self.db.execute(
            "SELECT * FROM ops.files WHERE project=%s AND sha256=%s", (project, sha)
        ).fetchone()
        if not row:
            raise ValueError("Unknown source file")
        if apply and row["retired_at"] is None:
            running = self.db.execute(
                "SELECT 1 FROM ops.runs WHERE project=%s AND status='running' LIMIT 1", (project,)
            ).fetchone()
            if running:
                raise RuntimeError("Wait for active runs to finish before retiring a source file")
            self.db.execute(
                "UPDATE ops.files SET retired_at=now(), retirement_reason=%s, needs_rebuild=true "
                "WHERE project=%s AND sha256=%s",
                (reason, project, sha),
            )
        return {
            "file_sha256": sha,
            "original_name": row["original_name"],
            "apply": apply,
            "action": "Retire from next Bronze/transform run; raw object is retained",
        }

    @contextmanager
    def stage(self, key, name):
        self.assert_current(key)
        self.db.execute(
            "UPDATE ops.runs SET status='running', finished_at=NULL WHERE run_key=%s", (key,)
        )
        attempt = self.db.execute(
            "INSERT INTO ops.stages(run_key, stage, status) VALUES (%s,%s,'running') RETURNING id",
            (key, name),
        ).fetchone()["id"]
        started = time.monotonic()
        metrics = {}
        try:
            yield metrics
        except BaseException as exc:
            self.db.execute(
                "UPDATE ops.stages SET status='failed', finished_at=now(), duration_seconds=%s, "
                "error_type=%s WHERE id=%s",
                (time.monotonic() - started, type(exc).__name__, attempt),
            )
            self.db.execute(
                "UPDATE ops.runs SET status='failed', finished_at=now() WHERE run_key=%s", (key,)
            )
            raise
        else:
            self.db.execute(
                "UPDATE ops.stages SET status='success', finished_at=now(), duration_seconds=%s, "
                "metrics=%s::jsonb WHERE id=%s",
                (time.monotonic() - started, json.dumps(metrics, default=str), attempt),
            )
            self.db.execute(
                "UPDATE ops.runs SET metrics=metrics || %s::jsonb WHERE run_key=%s",
                (json.dumps({name: metrics}, default=str), key),
            )
