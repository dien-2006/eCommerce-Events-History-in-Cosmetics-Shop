#!/usr/bin/env python3
"""Start an isolated local Thrift/Iceberg server, run tests, clean up owned resources."""

import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pyspark
from pyhive import hive

ROOT = Path(__file__).resolve().parents[1]


def main():
    artifacts = ROOT / "artifacts/integration"
    artifacts.mkdir(parents=True, exist_ok=True)
    jar = ROOT / "spark/jars/iceberg-spark-runtime-3.5_2.12-1.10.0.jar"
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = {
        **os.environ,
        "SPARK_HOME": str(Path(pyspark.__file__).parent),
        "PYSPARK_PYTHON": sys.executable,
        "SPARK_LOCAL_IP": "127.0.0.1",
        "SPARK_THRIFT_HOST": "localhost",
        "SPARK_THRIFT_PORT": str(port),
        "RUN_SPARK_INTEGRATION": "1",
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    with tempfile.TemporaryDirectory(prefix="cosmetics-spark-test-") as work:
        common = ["--master", "local[2]", "--driver-class-path", str(jar), "--jars", str(jar)]
        configuration = {
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.lakehouse.type": "hadoop",
            "spark.sql.catalog.lakehouse.warehouse": Path(work, "warehouse").as_uri(),
            "spark.sql.catalog.lakehouse.cache-enabled": "false",
            "spark.sql.defaultCatalog": "lakehouse",
            "spark.sql.shuffle.partitions": "2",
            "spark.sql.session.timeZone": "UTC",
            "spark.ui.enabled": "false",
            "spark.sql.csv.parser.columnPruning.enabled": "false",
            "spark.sql.thriftServer.incrementalCollect": "true",
        }
        for key, value in configuration.items():
            common.extend(["--conf", f"{key}={value}"])
        with (artifacts / "init.log").open("w") as log:
            subprocess.run(
                [
                    str(ROOT / ".venv/bin/spark-sql"),
                    *common,
                    "--driver-memory",
                    "512m",
                    "-e",
                    "CREATE NAMESPACE IF NOT EXISTS lakehouse.default;",
                ],
                cwd=work,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
                timeout=120,
            )
        with (artifacts / "spark-thrift.log").open("w") as log:
            process = subprocess.Popen(
                [
                    str(ROOT / ".venv/bin/spark-submit"),
                    *common,
                    "--driver-memory",
                    "1536m",
                    "--class",
                    "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
                    "--hiveconf",
                    "hive.server2.thrift.bind.host=127.0.0.1",
                    "--hiveconf",
                    f"hive.server2.thrift.port={port}",
                    "--hiveconf",
                    "hive.server2.authentication=NOSASL",
                ],
                cwd=work,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            try:
                deadline = time.monotonic() + 120
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        raise RuntimeError(
                            "Spark exited; inspect artifacts/integration/spark-thrift.log"
                        )
                    try:
                        connection = hive.Connection(host="localhost", port=port, auth="NOSASL")
                        connection.close()
                        break
                    except Exception:
                        time.sleep(1)
                else:
                    raise TimeoutError("Spark Thrift readiness timed out")
                subprocess.run(
                    [sys.executable, "-m", "pytest", "tests/integration", "-q"],
                    cwd=ROOT,
                    env=env,
                    check=True,
                    timeout=900,
                )
            finally:
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
