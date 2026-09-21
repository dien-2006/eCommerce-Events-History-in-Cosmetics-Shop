"""Shared entry point for Airflow tasks and operator commands."""

import argparse
import json
import logging
import os
from datetime import UTC, datetime

from lakehouse import connections
from lakehouse.bronze import load_bronze
from lakehouse.config import as_of_date, digest, load_projects
from lakehouse.landing import land
from lakehouse.publish import publish
from lakehouse.registry import Registry, run_key
from lakehouse.transform import transform

STAGES = ("land", "bronze", "transform", "publish", "finish")


def execute(stage, project, orchestrator_id, options):
    with connections.postgres() as db:
        registry = Registry(db)
        registry.initialize()
        with registry.lock(project.name):
            key = (
                registry.begin(project.name, orchestrator_id, options)
                if stage == "land"
                else run_key(project.name, orchestrator_id)
            )
            run = registry.run(key)
            if run["status"] == "success":
                return {"run_key": key, "already_finished": True}
            with registry.stage(key, stage) as metrics:
                if stage == "land":
                    metrics.update(land(project, registry, key, connections.object_store()))
                elif stage in ("bronze", "transform", "publish"):
                    with connections.spark_cursor() as cursor:
                        if stage == "bronze":
                            metrics.update(load_bronze(project, registry, key, cursor))
                        elif stage == "transform":
                            metrics.update(
                                transform(
                                    project, registry, key, cursor, connections.object_store()
                                )
                            )
                        else:
                            client = connections.clickhouse()
                            try:
                                metrics.update(publish(project, registry, key, cursor, client))
                            finally:
                                client.close()
                elif stage == "finish":
                    registry.require_stage(key, "publish")
                    report = registry.run(key)
                    report["files"] = registry.files(key)
                    report["stages"] = registry.db.execute(
                        "SELECT stage,status,started_at,finished_at,duration_seconds,metrics "
                        "FROM ops.stages WHERE run_key=%s AND stage<>'finish' ORDER BY id",
                        (key,),
                    ).fetchall()
                    report["finished_at"] = datetime.now(UTC).isoformat()
                    # Upload before marking success. Report upload failures remain retryable.
                    connections.object_store().put_object(
                        Bucket=os.environ["S3_BUCKET"],
                        Key=f"artifacts/{project.name}/{key}/run.json",
                        Body=json.dumps({**report, "status": "success"}, default=str).encode(),
                        ContentType="application/json",
                    )
                    registry.db.execute(
                        "UPDATE ops.runs SET status='success', finished_at=now() WHERE run_key=%s",
                        (key,),
                    )
                    metrics["artifact"] = f"artifacts/{project.name}/{key}/run.json"
            return {"run_key": key, "stage": stage, **metrics}


def main():
    parser = argparse.ArgumentParser(description="Audited ELT pipeline")
    parser.add_argument("stage", choices=(*STAGES, "run", "status", "retire-file"))
    parser.add_argument("--project", default="cosmetics")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--as-of-date", default=None)
    parser.add_argument("--full-refresh", action="store_true")
    parser.add_argument("--file-sha")
    parser.add_argument("--reason")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    project = load_projects()[args.project]
    if args.stage == "retire-file":
        if not args.file_sha or not args.reason or len(args.reason.strip()) < 10:
            parser.error(
                "retire-file requires --file-sha and a meaningful --reason (>=10 characters)"
            )
        with connections.postgres() as db:
            registry = Registry(db)
            registry.initialize()
            with registry.lock(project.name):
                print(
                    json.dumps(
                        registry.retire_file(
                            project.name, digest(args.file_sha), args.reason.strip(), args.apply
                        )
                    )
                )
        return
    if args.stage == "status":
        with connections.postgres() as db:
            registry = Registry(db)
            registry.initialize()
            rows = db.execute(
                "SELECT run_key, orchestrator_run_id, status, created_at, finished_at, metrics "
                "FROM ops.runs WHERE project=%s ORDER BY sequence_id DESC LIMIT 10",
                (project.name,),
            ).fetchall()
            print(json.dumps(rows, default=str, indent=2))
        return
    if not args.run_id:
        parser.error("--run-id is required; use the same ID only to retry the same run")
    options = {"as_of_date": as_of_date(args.as_of_date), "full_refresh": args.full_refresh}
    for stage in STAGES if args.stage == "run" else (args.stage,):
        print(json.dumps(execute(stage, project, args.run_id, options), default=str))


if __name__ == "__main__":
    main()
