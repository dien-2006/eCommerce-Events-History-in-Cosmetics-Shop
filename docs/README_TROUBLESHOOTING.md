# Troubleshooting

## Iceberg REST catalog not responding / Spark can't create tables
Fallback option for student demo:
- Use Spark HadoopCatalog instead of REST for ingestion (still keeps MinIO + Iceberg tables)

Example Spark conf overrides:
- spark.sql.catalog.hc=org.apache.iceberg.spark.SparkCatalog
- spark.sql.catalog.hc.type=hadoop
- spark.sql.catalog.hc.warehouse=s3a://cosmetics-lakehouse/warehouse

Then write/read using `hc.<db>.<table>`.

## MinIO path-style access errors
Confirm these are set:
- fs.s3a.path.style.access=true
- fs.s3a.connection.ssl.enabled=false
- endpoint=http://minio:9000

FILE: docs/schema_evolution_notes.md

# Schema Evolution: `payment_method` appears on day T

## Goal
A new column appears in raw files; Bronze append should succeed; Silver/Gold should not break.

## Iceberg + Spark
- Iceberg supports adding columns without rewriting old data.
- Spark write options used:
  - spark.sql.iceberg.merge-schema=true
  - spark.sql.parquet.mergeSchema=true

## dbt handling
- Set `on_schema_change: append_new_columns` for models.
- In SQL models, select `payment_method` explicitly and allow nulls.
- If new columns appear, dbt incremental append continues to work.

## Demo script
1) Load `batch_001` old CSV without payment_method
2) Load `batch_002` new CSV with payment_method
3) Run `dbt build`
4) Query Silver: show payment_method is NULL for batch_001 and populated for batch_002