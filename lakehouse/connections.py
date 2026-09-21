"""Credentials are read at runtime, never embedded in SQL artifacts or log messages."""

import os
from contextlib import contextmanager


def object_store():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
    )


def postgres():
    import psycopg
    from psycopg.rows import dict_row

    return psycopg.connect(os.environ["OPS_DATABASE_URL"], row_factory=dict_row, autocommit=True)


@contextmanager
def spark_cursor():
    from pyhive import hive

    connection = hive.Connection(
        host=os.environ.get("SPARK_THRIFT_HOST", "spark-thrift"),
        port=int(os.environ.get("SPARK_THRIFT_PORT", "10000")),
        username="pipeline",
        auth="NOSASL",
        database="default",
        configuration={"spark.sql.session.timeZone": "UTC"},
    )
    cursor = connection.cursor()
    try:
        yield cursor
    finally:
        cursor.close()
        connection.close()


def clickhouse():
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "pipeline"),
        password=os.environ["CLICKHOUSE_PASSWORD"],
        connect_timeout=15,
        send_receive_timeout=600,
    )
