-- Optional: query Iceberg Gold directly (read-only) via table function `iceberg`
-- This is version-dependent; if it fails, use the "copy into MergeTree" approach instead.

-- Example placeholders (edit bucket/path after first Gold build):
-- SELECT * FROM iceberg('http://minio:9000/cosmetics-lakehouse/warehouse/cosmetics_gold.db/daily_sales_by_category');