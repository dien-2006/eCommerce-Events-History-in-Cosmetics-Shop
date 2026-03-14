# Implementation Plan (Student-Friendly)

## Repo folder structure (EPO required)
- docker/
  - (reserved for future custom images / compose overrides)
- spark/
  - conf/
    - spark-defaults.conf
  - jars/
  - jobs/
    - bronze_ingest.py
    - load_gold_to_clickhouse.py
- dbt/
  - dbt_project.yml
  - profiles.yml.example
  - packages.yml
  - models/
    - staging/
      - stg_events.sql
      - schema.yml
    - gold/
      - funnel_steps_daily.sql
      - daily_sales_by_category.sql
      - rfm_segments.sql
      - schema.yml
- clickhouse/
  - config.d/
    - minio_s3.xml
  - initdb/
    - 01_databases.sql
    - 02_gold_tables.sql
    - 03_iceberg_external_views.sql
- superset/
  - Dockerfile
  - requirements.txt
  - superset_config.py
  - init/
    - superset_init.sh
  - BUILD_GUIDE.md
- scripts/
  - create_bucket.sh
  - download_jars.sh
  - verify_services.sh
- docs/
  - implementation_plan.md
  - data_contract.md
  - demo_runbook.md
  - validation_checklist.md

---

## Milestones (steps 1..5)

### Step 1 — Infra up on one docker network
**Deliverables**
- `docker-compose.yml`, `.env`, `scripts/verify_services.sh`
- services boot: MinIO + bucket, Iceberg REST, Spark master + 2 workers, ClickHouse, Superset (+ postgres + redis)

**Definition of Done**
- `docker compose up -d --build` succeeds
- `bash scripts/verify_services.sh` prints OK for all services
- Spark UI reachable; ClickHouse responds to `/ping`; Superset `/health` returns JSON

---

### Step 2 — Bronze ingestion to Iceberg on MinIO
**Deliverables**
- `spark/jobs/bronze_ingest.py`
- documented mounts: `./data -> /data`, `./spark/jars -> /opt/spark/jars`

**Definition of Done**
- running spark-submit loads Kaggle CSV from `./data`
- Bronze Iceberg table exists and has rows
- bad rows (if any) written to `/data/_quarantine/...` with counts printed

---

### Step 3 — Silver + Gold in dbt (dbt-spark)
**Deliverables**
- dbt project with Silver staging + Gold marts
- tests: not_null, unique, accepted_values
- docs generation command

**Definition of Done**
- `dbt build` runs successfully (staging + gold)
- `dbt test` passes
- schema evolution demo: adding `payment_method` to Bronze does not break dbt

---

### Step 4 — ClickHouse integration (two paths)
**Deliverables**
- ClickHouse config for S3/MinIO access
- external Iceberg read examples (table function / engine)
- Spark job to copy Gold into MergeTree tables

**Definition of Done**
- ClickHouse can run at least 3 queries against Gold:
  - revenue trend
  - top categories
  - funnel conversion
- Copy job loads data into MergeTree with partition-by day and ORDER BY
- validation counts match Iceberg source (within expected incremental window)

---

### Step 5 — Superset dashboard + presentation demo
**Deliverables**
- Superset built with clickhouse-connect
- build guide with 3 interactive charts
- demo runbook + validation checklist

**Definition of Done**
- dashboard exists with filters (date range, category, brand)
- screenshots checklist completed
- demo run can be executed end-to-end in <= 10 minutes

---

## Top 10 risks + mitigations
1) **Iceberg REST image env mismatch** → provide fallback: use Spark HadoopCatalog (non-REST) for ingestion + keep REST for interop demo.
2) **JAR download blocked** → commit jars to repo for demo day or host them in MinIO and curl from there.
3) **MinIO credentials / path-style issues** → keep `path.style.access=true`, `ssl=false`, verify with `mc ls`.
4) **Spark container permissions / mounts** → keep mounts under project folder, avoid root-owned host paths.
5) **Schema evolution breaks dbt** → select columns defensively; use `on_schema_change=append_new_columns` and `mergeSchema`.
6) **ClickHouse Iceberg support differs by version** → stick to official stable image and use Iceberg table function; keep troubleshooting notes.
7) **Superset init flakiness** → run init script idempotently; keep admin credentials deterministic.
8) **Performance demo too slow** → restrict demo dataset size, use daily partitions and ORDER BY, run `ANALYZE` where applicable.
9) **Team merge conflicts** → enforce branch-per-step; PR review checklist per milestone.
10) **Time constraints** → lock “minimum viable demo” scope: Bronze->Silver->Gold + 3 Superset charts.