#!/usr/bin/env python3
"""Exercise the data plane in disposable Docker volumes with a tiny synthetic dataset.

This intentionally tests the pipeline CLI, not Airflow scheduling or the BI UI.
Requires Docker Compose >=2.24.4 and built Spark/Airflow images.
"""

import hashlib
import json
import os
import subprocess
import tempfile
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HEADER = "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session\n"


def main():
    name = "cosmetics_test_" + uuid.uuid4().hex[:8]
    logs = ROOT / "artifacts" / name
    logs.mkdir(parents=True)
    with tempfile.TemporaryDirectory(prefix="cosmetics-stack-") as directory:
        work = Path(directory)
        raw = work / "raw"
        raw.mkdir()
        (raw / "first.csv").write_text(
            HEADER
            + "2020-01-01 10:00:00 UTC,view,1,1,skin,brand,10,1,s1\n"
            + "2020-01-01 10:01:00 UTC,cart,1,1,skin,brand,10,1,s1\n"
            + "2020-01-01 10:02:00 UTC,purchase,1,1,skin,brand,10,1,s1\n"
        )
        override = work / "compose.yml"
        override.write_text("""services:
  minio:
    ports: !reset []
  clickhouse:
    ports: !reset []
  spark-thrift:
    ports: !reset []
    environment:
      SPARK_DRIVER_MEMORY: 1536m
      SPARK_LOCAL_CORES: '2'
""")
        compose = [
            "docker", "compose", "--env-file", str(ROOT / ".env.deploy"),
            "-p", name, "-f", str(ROOT / "docker-compose.yml"), "-f", str(override),
        ]
        env = {**os.environ, "RAW_DATA_DIR": str(raw)}

        def run(label, *args, success=True):
            with (logs / f"{label}.log").open("w") as stream:
                result = subprocess.run(
                    [*compose, *args], cwd=ROOT, env=env, stdout=stream,
                    stderr=subprocess.STDOUT, timeout=900,
                )
            if success and result.returncode:
                raise RuntimeError(f"{label} failed; inspect {logs / (label + '.log')}")
            if not success and not result.returncode:
                raise AssertionError(f"{label} unexpectedly succeeded")
            print(f"{label}: expected exit status confirmed", flush=True)

        prefix = [
            "run", "--rm", "--no-deps", "-T", "--entrypoint",
            "/opt/pipeline-venv/bin/python", "airflow-scheduler",
        ]

        def pipeline(label, *args, success=True):
            run(label, *prefix, "-m", "lakehouse.cli", *args, success=success)

        def verify(label, revenue, release):
            key = hashlib.sha256(f"cosmetics\0{release}".encode()).hexdigest()
            code = f"""
from decimal import Decimal
from lakehouse.connections import clickhouse, object_store, postgres
import os
c = clickhouse()
assert c.query('SELECT sum(revenue) FROM cosmetics_analytics.daily_sales_by_category').result_rows[0][0] == Decimal('{revenue}')
assert c.query('SELECT release_id FROM cosmetics_analytics.current_release').result_rows == [('{key}',)]
with postgres() as db:
    assert db.execute("SELECT status FROM ops.runs WHERE run_key=%s", ('{key}',)).fetchone()['status'] == 'success'
s = object_store()
s.head_object(Bucket=os.environ['S3_BUCKET'], Key='artifacts/cosmetics/{key}/run.json')
c.close()
print('Serving totals, release pointer, registry and raw-store run artifact verified')
"""
            run(label, *prefix, "-c", code)

        try:
            run("startup", "up", "-d", "--wait", "--wait-timeout", "600", "spark-thrift", "clickhouse")
            pipeline("first", "run", "--run-id", "first")
            verify("first_check", "10", "first")
            pipeline("retry", "run", "--run-id", "first")
            verify("retry_check", "10", "first")
            pipeline("replay", "run", "--run-id", "replay")
            verify("replay_check", "10", "replay")
            (raw / "late.csv").write_text(
                HEADER + "2019-12-01 10:00:00 UTC,purchase,2,1,skin,brand,5,2,s2\n"
            )
            pipeline("late", "run", "--run-id", "late")
            verify("late_check", "15", "late")
            bad = HEADER + "invalid,purchase,2,1,skin,brand,5,2,s3\n"
            (raw / "bad.csv").write_text(bad)
            pipeline("quality_gate", "run", "--run-id", "bad", success=False)
            verify("quality_check", "15", "late")
            pipeline(
                "retire", "retire-file", "--file-sha", hashlib.sha256(bad.encode()).hexdigest(),
                "--reason", "Synthetic invalid source removed by acceptance test", "--apply",
            )
            pipeline("recovery", "run", "--run-id", "recovery")
            verify("recovery_check", "15", "recovery")
            first_key = hashlib.sha256(b"cosmetics\0first").hexdigest()
            run("rollback", *prefix, "-m", "lakehouse.releases", "--release", first_key, "--apply")
            verify("rollback_check", "10", "first")
            (logs / "result.json").write_text(json.dumps({"status": "passed", "project": name}))
            print(f"Container data-plane acceptance passed. Evidence: {logs}", flush=True)
        finally:
            try:
                run("services", "ps", "--all")
                run("service_logs", "logs", "--no-color", "--tail", "200")
            finally:
                # Only this invocation's randomly named test volumes are removed.
                run("cleanup", "down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
