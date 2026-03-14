# End-to-End Validation Checklist (Grading Rubric)

## Infra/DevOps (prove stable + no connection errors)
- [ ] `docker compose up -d --build` completes
- [ ] `bash scripts/verify_services.sh` returns OK for all
- [ ] MinIO console accessible; bucket exists
- [ ] Spark master shows 2 workers registered
- [ ] ClickHouse `/ping` responds "Ok."
- [ ] Superset `/health` returns JSON OK

Evidence to capture:
- screenshots of UIs + terminal outputs

---

## Medallion (Bronze/Silver/Gold correctness)
### Bronze
- [ ] Table exists: `rest.cosmetics_bronze.events_history`
- [ ] Rowcount > 0
- [ ] Metadata columns populated
- [ ] Quarantine folder exists if corrupt rows detected

### Silver
- [ ] `rest.cosmetics_silver.stg_events` exists
- [ ] `event_ts` parsed, `event_date` derived
- [ ] Dedup key unique

### Gold
- [ ] 3 gold tables exist and have rows
- [ ] Totals reconcile:
  - purchases in Silver vs orders sum in daily_sales_by_category (close match)
- [ ] Partition pruning works by date filter

---

## Spark/dbt (tests + docs + schema evolution demo script)
### dbt tests
- [ ] `dbt test` passes (unique/not_null/accepted_values)

### dbt docs
- [ ] `dbt docs generate` succeeds
- [ ] open docs site (optional for demo)

### Schema evolution demo (payment_method appears day T)
Script:
1) Ingest old batch without payment_method (already supported)
2) Add new CSV with extra column `payment_method`
3) Run ingestion again (append)
4) Run `dbt build` again; models should not break
- [ ] Confirm column present in Silver (nullable for old records)

---

## ClickHouse performance (PK/indices + benchmark queries)
- [ ] Explain ORDER BY / PARTITION BY design
- [ ] Benchmark queries (record time):
  - revenue trend by date
  - top categories
  - funnel conversion
- [ ] show `EXPLAIN` output or query timings

---

## Superset (screenshots checklist + KPI correctness)
- [ ] DB connection test successful
- [ ] Datasets created from ClickHouse tables
- [ ] 3 interactive charts created
- [ ] Dashboard filters work
- [ ] KPI values match ClickHouse SQL queries

---

## Demo runbook (presentation day)
See `docs/demo_runbook.md`.