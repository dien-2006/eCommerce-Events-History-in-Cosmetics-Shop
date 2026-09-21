#!/usr/bin/env python3
"""Run in an Airflow Python environment; validate imports and serialization."""

import os
from pathlib import Path

from airflow.models.dagbag import DagBag
from airflow.serialization.serialized_objects import SerializedDAG

root = Path(__file__).resolve().parents[1]
os.environ.setdefault("PROJECT_CONFIG", str(root / "config/projects.yml"))
bag = DagBag(str(root / "airflow/dags"), include_examples=False)
assert not bag.import_errors, bag.import_errors
assert bag.dags, "No project DAGs found"
for dag in bag.dags.values():
    assert dag.max_active_runs == 1
    assert dag.task_ids == ["land", "bronze", "transform", "publish", "finish"]
    assert dag.get_task("publish").upstream_task_ids == {"transform"}
    SerializedDAG.to_dict(dag)
    print(f"Validated {dag.dag_id}: {', '.join(dag.task_ids)}")
