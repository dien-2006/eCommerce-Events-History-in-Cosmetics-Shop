# Superset Dashboard Build Guide (ClickHouse)

## 1) Add ClickHouse database connection
Superset UI:
- Settings → Database Connections → + Database
- Choose: ClickHouse
- SQLAlchemy URI (HTTP):
  clickhouse+connect://{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}@clickhouse:8123/{CLICKHOUSE_DB}

Example (from .env defaults):
  clickhouse+connect://default:changeme@clickhouse:8123/analytics

Test Connection → Save.

## 2) Create datasets
Datasets to add:
1) `analytics.funnel_steps_daily`
2) `analytics.daily_sales_by_category`
3) `analytics.rfm_segments`

## 3) Create charts (3 required)

### Chart A — Conversion Funnel over time (daily)
- Dataset: `analytics.funnel_steps_daily`
- Chart type: Line Chart (or Time-series Line)
- Time column: `event_date`
- Metrics:
  - `users_view`
  - `users_cart`
  - `users_purchase`
  - optional: `conv_view_to_purchase` as secondary metric
- Filters:
  - Time range (native time filter)

### Chart B — Revenue trend + breakdown
- Dataset: `analytics.daily_sales_by_category`
- Chart type: Time-series Area (stacked) or Bar chart
- Time column: `event_date`
- Dimension (group by): `category_code`
- Metric: `revenue`
- Filters:
  - Time range
  - category_code (dropdown)

Optional second chart from same dataset:
- Chart type: Table
- Columns: category_code, revenue, orders, aov
- Sort: revenue desc

### Chart C — RFM segments distribution + revenue contribution
If you also store monetary per segment, you can:
- Dataset: `analytics.rfm_segments`
- Chart type: Pie Chart (or Bar)
- Dimension: `segment`
- Metrics:
  - `countDistinct(user_id)` as Users
  - optional: `sum(monetary)` as Revenue (if present in table)
- Filters:
  - snapshot_date (latest) using a filter or a default query

## 4) Dashboard
- Dashboards → + Dashboard
- Add 3 charts above
- Add filter controls:
  - Date range (event_date)
  - category_code
  - segment

## 5) Screenshot checklist (for grading)
- Dashboard overview
- Filters panel expanded
- Each chart drill-down / tooltip visible
- SQL Lab query example for KPI validation