"""Run dbt in a subprocess and persist artifacts in object storage."""

import json
import os
import subprocess
from pathlib import Path


def transform(project, registry, key, cursor, s3):
    registry.require_stage(key, "bronze")
    cursor.execute(f"SELECT count(*) FROM {project.bronze}.events_history")
    if cursor.fetchone()[0] == 0:
        raise RuntimeError("Bronze is empty. Add input CSV files before building marts.")
    options = registry.run(key)["options"]
    rebuild = (
        registry.db.execute(
            "SELECT count(*) AS n FROM ops.files WHERE project=%s AND needs_rebuild",
            (project.name,),
        ).fetchone()["n"]
        > 0
    )
    variables = {"project_name": project.name, "max_invalid_ratio": project.max_invalid_ratio}
    if options.get("as_of_date"):
        variables["as_of_date"] = options["as_of_date"]
    target = Path(os.environ.get("ARTIFACTS_DIR", "/opt/airflow/artifacts")) / project.name / key
    target.mkdir(parents=True, exist_ok=True)
    executable = os.environ.get("DBT_EXECUTABLE", "/opt/pipeline-venv/bin/dbt")
    common = [
        "--project-dir",
        str(project.dbt_dir),
        "--profiles-dir",
        str(project.dbt_dir),
        "--target-path",
        str(target),
        "--log-path",
        str(target / "logs"),
        "--vars",
        json.dumps(variables),
    ]
    try:
        flags = ["--full-refresh"] if rebuild or options.get("full_refresh") else []
        subprocess.run([executable, "build", *flags, *common], check=True, timeout=7200)
        registry.db.execute(
            "UPDATE ops.files SET needs_rebuild=false WHERE project=%s", (project.name,)
        )
        subprocess.run([executable, "docs", "generate", *common], check=True, timeout=1800)
    finally:
        for name in ("manifest.json", "run_results.json", "catalog.json", "index.html"):
            artifact = target / name
            if artifact.exists():
                s3.upload_file(
                    str(artifact),
                    os.environ["S3_BUCKET"],
                    f"artifacts/{project.name}/{key}/dbt/{name}",
                )
    cursor.execute(f"SELECT count(*) FROM {project.silver}.quarantine_events")
    invalid = cursor.fetchone()[0]
    cursor.execute(
        f"SELECT count(*), cast(max(event_date) as string) FROM {project.silver}.stg_events"
    )
    valid, latest = cursor.fetchone()
    return {
        "silver_rows": valid,
        "quarantine_rows": invalid,
        "latest_event_date": latest,
        "full_refresh": bool(rebuild or options.get("full_refresh")),
    }
