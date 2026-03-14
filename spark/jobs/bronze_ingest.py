import argparse
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    input_file_name,
    lit,
    to_timestamp,
)
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
)

EXPECTED_COLS = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
    # future schema evolution example:
    "payment_method",
]

def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # allow adding new columns (schema evolution) when writing
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .config("spark.sql.parquet.mergeSchema", "true")
        .config("spark.sql.iceberg.merge-schema", "true")
        .getOrCreate()
    )

def detect_files(root: str):
    exts = (".csv", ".json")
    paths = []
    for dirpath, _, filenames in os.walk(root):
        for f in filenames:
            if f.lower().endswith(exts):
                paths.append(os.path.join(dirpath, f))
    return sorted(paths)

def read_raw(spark: SparkSession, paths):
    # permissive mode: keep corrupt rows for quarantine
    return (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("inferSchema", "false")
        .csv([p for p in paths if p.lower().endswith(".csv")])
    )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input folder mounted as /data (batch arrivals)")
    parser.add_argument("--catalog", default="rest")
    parser.add_argument("--db", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--quarantine", default="/data/_quarantine", help="Quarantine output path (mounted)")
    args = parser.parse_args()

    spark = build_spark("bronze_ingest")

    files = detect_files(args.input)
    if not files:
        raise SystemExit(f"No input files found under: {args.input}")

    csv_files = [f for f in files if f.lower().endswith(".csv")]
    if not csv_files:
        raise SystemExit("This minimal job currently supports CSV inputs only. Put Kaggle CSV under ./data/")

    df = read_raw(spark, csv_files)

    # ensure expected columns exist (schema evolution safe)
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df = df.withColumn(c, lit(None).cast(StringType()))

    ingestion_time = datetime.now(timezone.utc).isoformat()

    enriched = (
        df
        .withColumn("_ingestion_time", lit(ingestion_time))
        .withColumn("_source_file", input_file_name())
        .withColumn("_batch_id", lit(args.batch_id))
    )

    # quarantine corrupt rows
    bad = enriched.filter(col("_corrupt_record").isNotNull())
    good = enriched.filter(col("_corrupt_record").isNull())

    bad_count = bad.count()
    good_count = good.count()

    if bad_count > 0:
        (
            bad.coalesce(1)
            .write.mode("append")
            .parquet(os.path.join(args.quarantine, f"batch_id={args.batch_id}"))
        )

    # Bronze table: mostly raw strings, but keep numeric ids as string-safe then cast later in Silver
    bronze = (
        good.select(
            col("event_time").cast(StringType()).alias("event_time"),
            col("event_type").cast(StringType()).alias("event_type"),
            col("product_id").cast(StringType()).alias("product_id"),
            col("category_id").cast(StringType()).alias("category_id"),
            col("category_code").cast(StringType()).alias("category_code"),
            col("brand").cast(StringType()).alias("brand"),
            col("price").cast(StringType()).alias("price"),
            col("user_id").cast(StringType()).alias("user_id"),
            col("user_session").cast(StringType()).alias("user_session"),
            col("payment_method").cast(StringType()).alias("payment_method"),
            col("_ingestion_time"),
            col("_source_file"),
            col("_batch_id"),
        )
    )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.catalog}.{args.db}")
    # create Iceberg table if missing
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {args.catalog}.{args.db}.{args.table} (
          event_time STRING,
          event_type STRING,
          product_id STRING,
          category_id STRING,
          category_code STRING,
          brand STRING,
          price STRING,
          user_id STRING,
          user_session STRING,
          payment_method STRING,
          _ingestion_time STRING,
          _source_file STRING,
          _batch_id STRING
        )
        USING iceberg
        PARTITIONED BY (_batch_id)
        """
    )

    (
        bronze.writeTo(f"{args.catalog}.{args.db}.{args.table}")
        .append()
    )

    print(f"[bronze_ingest] files={len(csv_files)} good={good_count} bad={bad_count}")
    print(f"[bronze_ingest] wrote: {args.catalog}.{args.db}.{args.table}")
    if bad_count > 0:
        print(f"[bronze_ingest] quarantined to: {args.quarantine}/batch_id={args.batch_id}")

    spark.stop()

if __name__ == "__main__":
    main()