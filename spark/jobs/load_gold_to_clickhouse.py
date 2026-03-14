import argparse
from pyspark.sql import SparkSession

def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", default="rest")
    p.add_argument("--db", required=True, help="Iceberg DB for gold tables")
    p.add_argument("--table", required=True, help="Iceberg gold table name")
    p.add_argument("--ch-host", default="clickhouse")
    p.add_argument("--ch-port", default="8123")
    p.add_argument("--ch-db", default="analytics")
    p.add_argument("--ch-user", default="default")
    p.add_argument("--ch-pass", default="changeme")
    p.add_argument("--ch-table", required=True, help="Target ClickHouse table")
    p.add_argument("--day", default=None, help="Incremental load filter YYYY-MM-DD on partition column event_date (if exists)")
    args = p.parse_args()

    spark = build_spark("gold_to_clickhouse")

    src = spark.table(f"{args.catalog}.{args.db}.{args.table}")
    df = src

    # incremental by day (if the table has event_date)
    if args.day and "event_date" in df.columns:
        df = df.filter(df["event_date"] == args.day)

    jdbc_url = f"jdbc:clickhouse://{args.ch_host}:{args.ch_port}/{args.ch_db}"
    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", args.ch_table)
        .option("user", args.ch_user)
        .option("password", args.ch_pass)
        .option("batchsize", "100000")
        .mode("append")
        .save()
    )

    print(f"[gold_to_clickhouse] wrote rows={df.count()} to {args.ch_db}.{args.ch_table}")
    spark.stop()

if __name__ == "__main__":
    main()