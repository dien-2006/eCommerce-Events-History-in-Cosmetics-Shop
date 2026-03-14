-- Examples for ClickHouse Iceberg read (edit the URL/path to match your table location)

-- 1) Table function (preferred by ClickHouse docs)
-- SELECT *
-- FROM iceberg('http://minio:9000/cosmetics-lakehouse/warehouse/cosmetics_gold.db/daily_sales_by_category');

-- 2) If your ClickHouse build supports Iceberg engine:
-- CREATE TABLE gold_sales
-- ENGINE = Iceberg('http://minio:9000/cosmetics-lakehouse/warehouse/cosmetics_gold.db/daily_sales_by_category');

-- Troubleshooting flags (set in server config):
-- - use_virtual_host_style=false
-- - correct credentials