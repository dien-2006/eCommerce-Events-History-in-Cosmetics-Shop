"""Explicit publication rollback with full reconciliation against the stored manifest."""

import argparse
import json

from lakehouse.config import digest, load_projects
from lakehouse.connections import clickhouse, postgres
from lakehouse.publish import MARTS, Fingerprint, current_release
from lakehouse.registry import Registry


def main():
    parser = argparse.ArgumentParser(description="Inspect or roll back serving releases")
    parser.add_argument("--project", default="cosmetics")
    parser.add_argument("--release", help="Validated SHA-256 run key to restore")
    parser.add_argument(
        "--apply", action="store_true", help="Apply the pointer swap after validation"
    )
    args = parser.parse_args()
    project = load_projects()[args.project]
    with postgres() as db:
        registry = Registry(db)
        with registry.lock(project.name):
            client = clickhouse()
            try:
                active = current_release(project, client)
                print(json.dumps({"current_release": active}))
                if not args.release:
                    rows = db.execute(
                        "SELECT run_key, created_at, status FROM ops.runs WHERE project=%s "
                        "AND metrics ? 'candidate' ORDER BY sequence_id DESC LIMIT 30",
                        (project.name,),
                    ).fetchall()
                    print(json.dumps(rows, default=str, indent=2))
                    return
                key = digest(args.release)
                row = db.execute(
                    "SELECT * FROM ops.runs WHERE run_key=%s AND project=%s", (key, project.name)
                ).fetchone()
                if not row or "candidate" not in row["metrics"]:
                    raise ValueError(
                        "No validated candidate manifest found for this project/release"
                    )
                for name, schema in MARTS.items():
                    actual = Fingerprint()
                    columns = ", ".join(c for c, _ in schema)
                    with client.query_row_block_stream(
                        f"SELECT {columns} FROM {project.serving}._versions_{name} WHERE _release_id='{key}'"
                    ) as stream:
                        for block in stream:
                            actual.add(block)
                    if actual.value() != row["metrics"]["candidate"]["tables"][name]:
                        raise RuntimeError(
                            f"Stored data does not match the validated manifest: {name}"
                        )
                if args.apply and key != active:
                    pointer = f"{project.serving}._rollback_pointer"
                    client.command(
                        f"CREATE TABLE IF NOT EXISTS {pointer} AS {project.serving}.current_release"
                    )
                    client.command(f"TRUNCATE TABLE {pointer}")
                    client.command(f"INSERT INTO {pointer} SELECT '{key}', now64(3)")
                    client.command(
                        f"EXCHANGE TABLES {project.serving}.current_release AND {pointer}"
                    )
                    db.execute(
                        "INSERT INTO ops.stages(run_key, stage, status, finished_at, metrics) "
                        "VALUES (%s,'rollback','success',now(),%s::jsonb)",
                        (key, json.dumps({"previous_release": active, "restored_release": key})),
                    )
                    print("Rollback applied and audited.")
                else:
                    print("Release validated. Use --apply to change the publication pointer.")
            finally:
                client.close()


if __name__ == "__main__":
    main()
