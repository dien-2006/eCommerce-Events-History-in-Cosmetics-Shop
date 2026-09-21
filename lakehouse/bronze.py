"""Load landed bytes into Iceberg without business transformations."""

import os

from lakehouse.config import OPTIONAL_COLUMNS, RAW_COLUMNS, digest, sql_string, validate_header


def create_bronze_sql(project):
    columns = ",\n".join(f"{name} STRING" for name in RAW_COLUMNS + OPTIONAL_COLUMNS)
    return f"""CREATE TABLE IF NOT EXISTS {project.bronze}.events_history (
      {columns}, _corrupt_record STRING, _file_sha256 STRING,
      _source_uri STRING, _ingested_at TIMESTAMP, _run_key STRING
    ) USING iceberg PARTITIONED BY (_file_sha256)
    TBLPROPERTIES ('format-version'='2')"""


def raw_view_sql(header, path):
    validate_header(header)
    schema = ", ".join(f"{c} STRING" for c in header) + ", _corrupt_record STRING"
    return f"""CREATE TEMPORARY VIEW raw_input ({schema}) USING csv OPTIONS (
      path {sql_string(path)}, header 'true', mode 'PERMISSIVE',
      columnNameOfCorruptRecord '_corrupt_record', enforceSchema 'false',
      multiLine 'false', encoding 'UTF-8'
    )"""


def overwrite_sql(project, header, sha, path, key):
    digest(sha)
    digest(key)
    select = [
        c if c in header else f"CAST(NULL AS STRING) AS {c}" for c in RAW_COLUMNS + OPTIONAL_COLUMNS
    ]
    return f"""INSERT OVERWRITE {project.bronze}.events_history
      SELECT {", ".join(select)}, _corrupt_record, '{sha}',
             {sql_string(path)}, current_timestamp(), '{key}'
      FROM raw_input"""


def retirement_sql(project, sha):
    return f"DELETE FROM {project.bronze}.events_history WHERE _file_sha256='{digest(sha)}'"


def load_bronze(project, registry, key, cursor):
    registry.require_stage(key, "land")
    cursor.execute(f"CREATE NAMESPACE IF NOT EXISTS {project.bronze}")
    cursor.execute(create_bronze_sql(project))
    cursor.execute("SET spark.sql.sources.partitionOverwriteMode=dynamic")
    retired = registry.db.execute(
        "SELECT sha256 FROM ops.files WHERE project=%s AND retired_at IS NOT NULL AND needs_rebuild",
        (project.name,),
    ).fetchall()
    for file in retired:
        # Retirement is explicit and audited in ops; immutable raw bytes are retained.
        cursor.execute(retirement_sql(project, file["sha256"]))
    rows, loaded = 0, 0
    for file in registry.files(key):
        if file["bronze_loaded_at"] or file["retired_at"]:
            continue
        sha = digest(file["sha256"])
        header = validate_header(file["header"])
        path = f"s3a://{os.environ['S3_BUCKET']}/{file['object_key']}"
        cursor.execute("DROP VIEW IF EXISTS raw_input")
        cursor.execute(raw_view_sql(header, path))
        # One atomic commit replaces only this content-addressed file partition.
        # Replay is safe if the process dies before the PostgreSQL checkpoint.
        cursor.execute(overwrite_sql(project, header, sha, path, key))
        cursor.execute(
            f"SELECT count(*) FROM {project.bronze}.events_history WHERE _file_sha256='{sha}'"
        )
        count = cursor.fetchone()[0]
        registry.db.execute(
            "UPDATE ops.files SET bronze_loaded_at=now(), row_count=%s WHERE project=%s AND sha256=%s",
            (count, project.name, sha),
        )
        loaded += 1
        rows += count
    return {"loaded_files": loaded, "loaded_rows": rows}
