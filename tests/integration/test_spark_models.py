"""Runs the actual Bronze SQL and dbt models against an isolated Iceberg namespace."""

import csv
import hashlib
import json
import os
import subprocess
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

from lakehouse.bronze import create_bronze_sql, overwrite_sql, raw_view_sql, retirement_sql
from lakehouse.config import RAW_COLUMNS, Project
from lakehouse.connections import spark_cursor

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]


def test_replay_late_data_quarantine_and_business_metrics(tmp_path):
    if os.environ.get("RUN_SPARK_INTEGRATION") != "1":
        pytest.skip("Set RUN_SPARK_INTEGRATION=1 and start an isolated Spark Thrift server")
    name = "qa_" + uuid.uuid4().hex[:8]
    project = Project(name, "tests", None, tmp_path, ROOT / "dbt", 0.2)

    def event(user, session, kind, ts, price="10.00"):
        return [ts + " UTC", kind, "1", "100", "makeup", "brand", price, str(user), session]

    first = [
        event(1, "s1", "view", "2020-01-01 23:58:00"),
        event(1, "s1", "cart", "2020-01-01 23:59:00"),
        event(1, "s1", "purchase", "2020-01-02 00:01:00"),
        event(1, "s1", "purchase", "2020-01-02 00:01:00"),
        event(2, "s2", "purchase", "2020-01-01 10:00:00", "20"),
        event(2, "s2", "view", "2020-01-01 11:00:00", "20"),
        event(3, "s3", "view", "2020-01-02 10:00:00", "30"),
        event(3, "s3", "cart", "2020-01-02 10:00:00", "30"),
        event(3, "s3", "purchase", "2020-01-02 10:01:00", "30"),
        event(4, "s4", "remove_from_cart", "2020-01-02 11:00:00"),
        event(5, "bad", "view", "bad-time"),
        event(1, "s5", "purchase", "2020-01-03 11:00:00", "100"),
    ]
    first[3][5] = "brand_z"  # Conflicting attribute, otherwise identical event key.
    logdir = ROOT / "artifacts/integration"
    logdir.mkdir(parents=True, exist_ok=True)

    def dbt_build(label, expect_success=True, ratio=0.2, full_refresh=False):
        with (logdir / f"{label}.log").open("w") as log:
            result = subprocess.run(
                [
                    str(ROOT / ".venv/bin/dbt"),
                    "build",
                    *(["--full-refresh"] if full_refresh else []),
                    "--project-dir",
                    str(ROOT / "dbt"),
                    "--profiles-dir",
                    str(ROOT / "dbt"),
                    "--target-path",
                    str(tmp_path / "target"),
                    "--log-path",
                    str(tmp_path / "logs"),
                    "--vars",
                    json.dumps({"project_name": name, "max_invalid_ratio": ratio}),
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=600,
            )
        assert (result.returncode == 0) == expect_success, (logdir / f"{label}.log").read_text()[
            -12000:
        ]

    with spark_cursor() as cursor:
        cursor.execute(f"CREATE NAMESPACE {project.bronze}")
        cursor.execute(create_bronze_sql(project))
        cursor.execute("SET spark.sql.sources.partitionOverwriteMode=dynamic")

        def load(rows, filename):
            path = tmp_path / filename
            with path.open("w", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(RAW_COLUMNS)
                writer.writerows(rows)
            sha = hashlib.sha256(path.read_bytes()).hexdigest()
            cursor.execute("DROP VIEW IF EXISTS raw_input")
            cursor.execute(raw_view_sql(list(RAW_COLUMNS), path.as_uri()))
            cursor.execute(overwrite_sql(project, RAW_COLUMNS, sha, path.as_uri(), "a" * 64))
            return sha

        def scalar(query):
            cursor.execute(query)
            return cursor.fetchone()[0]

        try:
            first_sha = load(first, "first.csv")
            load(first, "first.csv")
            assert scalar(f"SELECT count(*) FROM {project.bronze}.events_history") == 12
            dbt_build("first_build")
            assert scalar(f"SELECT count(*) FROM {project.silver}.stg_events") == 10
            assert scalar(f"SELECT count(*) FROM {project.silver}.quarantine_events") == 1
            assert (
                scalar(
                    f"SELECT brand FROM {project.silver}.stg_events "
                    "WHERE user_id=1 AND user_session='s1' AND event_type='purchase'"
                )
                == "brand_z"
            )
            assert scalar(
                f"SELECT sum(revenue) FROM {project.gold}.daily_sales_by_category"
            ) == Decimal("160")
            cursor.execute(
                f"SELECT event_date, sessions_purchase_after_cart FROM {project.gold}.funnel_steps_daily ORDER BY event_date"
            )
            assert cursor.fetchall() == [("2020-01-01", 1), ("2020-01-02", 1), ("2020-01-03", 0)]
            cursor.execute(
                f"SELECT r_score, f_score, m_score, segment FROM {project.gold}.rfm_segments WHERE user_id=1"
            )
            assert cursor.fetchone() == (5, 5, 5, "champions")
            dbt_build("replay_build")
            assert scalar(f"SELECT count(*) FROM {project.silver}.stg_events") == 10
            load([first[2], event(2, "late", "purchase", "2020-01-01 09:00:00", "5")], "late.csv")
            dbt_build("late_build")
            assert scalar(f"SELECT count(*) FROM {project.silver}.stg_events") == 11
            assert scalar(
                f"SELECT sum(revenue) FROM {project.gold}.daily_sales_by_category"
            ) == Decimal("165")
            dbt_build("quality_gate", expect_success=False, ratio=0)
            cursor.execute(retirement_sql(project, first_sha))
            dbt_build("source_repair", full_refresh=True, ratio=0)
            assert scalar(f"SELECT count(*) FROM {project.silver}.stg_events") == 2
            assert scalar(
                f"SELECT sum(revenue) FROM {project.gold}.daily_sales_by_category"
            ) == Decimal("15")
            with (logdir / "dbt_docs.log").open("w") as log:
                subprocess.run(
                    [
                        str(ROOT / ".venv/bin/dbt"),
                        "docs",
                        "generate",
                        "--project-dir",
                        str(ROOT / "dbt"),
                        "--profiles-dir",
                        str(ROOT / "dbt"),
                        "--target-path",
                        str(tmp_path / "target"),
                        "--log-path",
                        str(tmp_path / "logs"),
                        "--vars",
                        json.dumps({"project_name": name}),
                    ],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    check=True,
                    timeout=180,
                )
            assert (tmp_path / "target/catalog.json").is_file()
        finally:
            for schema in (project.gold, project.silver, project.bronze):
                cursor.execute(f"CREATE NAMESPACE IF NOT EXISTS {schema}")
                cursor.execute(f"SHOW TABLES IN {schema}")
                for _, table, temporary in cursor.fetchall():
                    if not temporary:
                        cursor.execute(f"DROP TABLE {schema}.{table} PURGE")
                cursor.execute(f"DROP NAMESPACE {schema}")
