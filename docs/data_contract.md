# Data Contract — REES46 Cosmetics eCommerce Events History (Purchase-only dataset)

## Source
Kaggle dataset: "eCommerce Events History in Cosmetics Shop"
- event_time (UTC string)
- event_type (purchase only, but treat as enum for future extension)
- product_id, category_id, category_code, brand, price
- user_id, user_session

## Naming conventions
- Catalog: `rest`
- Databases:
  - Bronze: `cosmetics_bronze`
  - Silver: `cosmetics_silver`
  - Gold: `cosmetics_gold`
- Tables:
  - Bronze raw: `events_history`
  - Silver: `stg_events`
  - Gold: `funnel_steps_daily`, `daily_sales_by_category`, `rfm_segments`

## S3 paths (MinIO)
- Warehouse root: `s3a://cosmetics-lakehouse/warehouse/`
- Iceberg managed tables:
  - `.../warehouse/<db>.db/<table>/`
- Quarantine:
  - `s3a://cosmetics-lakehouse/quarantine/events_history/`

---

## Bronze schema (raw + metadata)
**Table**: `rest.cosmetics_bronze.events_history`

| column | type | notes |
|---|---|---|
| event_time | STRING | raw, as-is |
| event_type | STRING | raw |
| product_id | STRING | raw |
| category_id | STRING | raw |
| category_code | STRING | raw nullable |
| brand | STRING | raw nullable |
| price | STRING | raw (cast later) |
| user_id | STRING | raw |
| user_session | STRING | raw |
| payment_method | STRING | nullable; example schema evolution column |
| _ingestion_time | STRING | ISO8601 UTC |
| _source_file | STRING | input file path |
| _batch_id | STRING | batch identifier |

**Bronze partition spec**
- `PARTITIONED BY (_batch_id)` for easy incremental demo and rollback by batch

---

## Silver schema (cleaned / typed)
**Table**: `rest.cosmetics_silver.stg_events`

| column | type | notes |
|---|---|---|
| event_ts | TIMESTAMP | parsed from event_time (UTC) |
| event_date | DATE | derived from event_ts |
| event_type | STRING | standardized enum: purchase/view/cart (future-proof) |
| product_id | BIGINT | cast |
| category_id | BIGINT | cast |
| category_code | STRING | normalized (lowercase) |
| brand | STRING | normalized (lowercase) |
| price | DOUBLE | currency assumed: dataset currency (treat as unit price) |
| user_id | BIGINT | cast |
| user_session | STRING | keep session id |
| payment_method | STRING | nullable; passed through if present |
| _ingestion_time | TIMESTAMP | casted from ISO8601 |
| _source_file | STRING | |
| _batch_id | STRING | |

**Silver dedup keys**
- natural key (recommended): `(user_id, user_session, product_id, event_ts)`
- rule: keep the latest `_ingestion_time` if duplicates exist

**Silver partition spec**
- `PARTITIONED BY (days(event_ts))` (or `event_date`) for query pruning

---

## Gold schema (marts)

### 1) funnel_steps_daily (future-proof)
**Table**: `rest.cosmetics_gold.funnel_steps_daily`
| column | type | notes |
|---|---|---|
| event_date | DATE | partition column |
| users_view | BIGINT | users who viewed |
| users_cart | BIGINT | users who carted |
| users_purchase | BIGINT | users who purchased |
| conv_view_to_cart | DOUBLE | users_cart/users_view |
| conv_cart_to_purchase | DOUBLE | users_purchase/users_cart |
| conv_view_to_purchase | DOUBLE | users_purchase/users_view |

**Partition spec**
- `PARTITIONED BY (days(event_date))` (or identity on date if used as DATE column)

### 2) daily_sales_by_category
**Table**: `rest.cosmetics_gold.daily_sales_by_category`
| column | type |
|---|---|
| event_date | DATE |
| category_code | STRING |
| revenue | DOUBLE |
| orders | BIGINT |
| aov | DOUBLE |

**Partition spec**
- `PARTITIONED BY (days(event_date))`
**Sort recommendation**
- sort within files by `(event_date, category_code)`

### 3) rfm_segments
**Table**: `rest.cosmetics_gold.rfm_segments`
| column | type |
|---|---|
| snapshot_date | DATE |
| user_id | BIGINT |
| recency_days | INT |
| frequency | BIGINT |
| monetary | DOUBLE |
| r_score | INT |
| f_score | INT |
| m_score | INT |
| segment | STRING |

**Partition spec**
- `PARTITIONED BY (days(snapshot_date))`
**Segmentation rules**
- simplest demo: quintiles per metric (1..5), segment label from RFM triple

---

## Partition recommendations summary
- Bronze: `_batch_id` (simple incremental loads)
- Silver: day(event_ts)
- Gold: day(event_date / snapshot_date)

---

## Currency assumptions
- `price` treated as unit price in dataset currency; no FX conversion in this project.