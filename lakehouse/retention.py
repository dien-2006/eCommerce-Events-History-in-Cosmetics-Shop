"""Dry-run-first retention for historical serving releases; never remove the active release."""

import argparse
import json
from datetime import UTC, datetime, timedelta

from lakehouse.config import digest, load_projects
from lakehouse.connections import clickhouse, postgres
from lakehouse.publish import MARTS, current_release
from lakehouse.registry import Registry


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="cosmetics")
    parser.add_argument("--keep", type=int, default=7)
    parser.add_argument("--older-than-days", type=int, default=7)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.keep < 2 or args.older_than_days < 1:
        parser.error("Keep at least two releases and at least one day")
    project = load_projects()[args.project]
    with postgres() as db:
        registry = Registry(db)
        with registry.lock(project.name):
            client = clickhouse()
            try:
                active = current_release(project, client)
                rows = db.execute(
                    "SELECT run_key, created_at FROM ops.runs WHERE project=%s "
                    "AND metrics ? 'candidate' ORDER BY sequence_id DESC",
                    (project.name,),
                ).fetchall()
                cutoff = datetime.now(UTC) - timedelta(days=args.older_than_days)
                candidates = [
                    r
                    for r in rows[args.keep :]
                    if r["run_key"] != active and r["created_at"] < cutoff
                ]
                print(
                    json.dumps(
                        {"active": active, "remove": candidates, "apply": args.apply}, default=str
                    )
                )
                if not args.apply:
                    return
                for row in candidates:
                    key = digest(row["run_key"])
                    # Recheck while holding the project lock; no publication can interleave.
                    if key == current_release(project, client):
                        raise RuntimeError("Refusing to remove active release")
                    for name in MARTS:
                        client.command(
                            f"ALTER TABLE {project.serving}._versions_{name} DROP PARTITION '{key}'"
                        )
                    client.command(f"DROP TABLE IF EXISTS {project.serving}._candidate_{key[:16]}")
                    db.execute(
                        "UPDATE ops.runs SET metrics=metrics - 'candidate' WHERE run_key=%s", (key,)
                    )
                    db.execute(
                        "INSERT INTO ops.stages(run_key, stage, status, finished_at) "
                        "VALUES (%s,'retention','success',now())",
                        (key,),
                    )
            finally:
                client.close()


if __name__ == "__main__":
    main()
