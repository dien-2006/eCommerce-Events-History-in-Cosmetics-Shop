"""One auditable DAG per configured commerce project; no network calls at parse time."""

import os
import subprocess
from datetime import timedelta

import pendulum

from airflow.sdk import DAG, Param, get_current_context, task
from lakehouse.config import as_of_date, load_projects


def run_stage(stage, project):
    context = get_current_context()
    selected_date = as_of_date(context["params"].get("as_of_date"))

    command = [
        "/opt/pipeline-venv/bin/python",
        "-m",
        "lakehouse.cli",
        stage,
        "--project",
        project,
        "--run-id",
        context["run_id"],
    ]

    if selected_date:
        command.extend(["--as-of-date", selected_date])
    if context["params"].get("full_refresh"):
        command.append("--full-refresh")
    # Argument lists, never shell interpolation of dag_run.conf.

    subprocess.run(command, check=True, timeout=10800)


for project in load_projects(
    os.environ.get("PROJECT_CONFIG", "/opt/project/config/projects.yml")
).values():
    with DAG(
        dag_id=f"{project.name}_elt",
        description=project.description,
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        schedule=project.schedule,
        catchup=False,
        max_active_runs=1,
        max_active_tasks=1,
        dagrun_timeout=timedelta(hours=8),
        tags=["elt", "iceberg", project.name],
        default_args={
            "owner": project.owner,
            "retries": 2,
            "retry_delay": timedelta(minutes=2),
            "retry_exponential_backoff": True,
            "execution_timeout": timedelta(hours=3),
            "pool": "lakehouse_writes",
        },
        params={
            "as_of_date": Param(
                None,
                type=["null", "string"],
                format="date",
                description="RFM snapshot date; default = latest event date",
            ),
            "full_refresh": Param(
                False, type="boolean", description="Rebuild Silver after a contract change"
            ),
        },
        doc_md="""## ELT with a publication quality gate
Raw CSV bytes → immutable MinIO objects → Bronze Iceberg → dbt build/tests →
reconciled ClickHouse release → atomic publication pointer → audit report.

Retry failed tasks with **Clear**. To pick up new files, trigger a new run.
The single-slot `lakehouse_writes` pool serializes projects on this Linux host.
See docs/operations.md for replay, quarantine, backup and rollback.
""",
    ) as dag:
        previous = None

        for stage in ("land", "bronze", "transform", "publish", "finish"):
            current = task(task_id=stage)(run_stage)(stage, project.name)
            if previous is not None:
                previous >> current
            previous = current

    globals()[dag.dag_id] = dag
