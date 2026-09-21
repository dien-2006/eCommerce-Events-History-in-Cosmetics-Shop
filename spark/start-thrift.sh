#!/usr/bin/env bash
set -euo pipefail
# Java properties are rendered explicitly. Docker does not interpolate mounted files.
python3 - <<'PYTHON'
import os
from pathlib import Path
properties = {
    'spark.sql.catalog.lakehouse.uri': 'jdbc:postgresql://postgres:5432/iceberg',
    'spark.sql.catalog.lakehouse.jdbc.user': 'iceberg',
    'spark.sql.catalog.lakehouse.jdbc.password': os.environ['ICEBERG_DB_PASSWORD'],
    'spark.sql.catalog.lakehouse.warehouse': 's3://' + os.environ['S3_BUCKET'] + '/warehouse',
    'spark.sql.catalog.lakehouse.s3.endpoint': os.environ['S3_ENDPOINT'],
    'spark.hadoop.fs.s3a.endpoint': os.environ['S3_ENDPOINT'],
    'spark.hadoop.fs.s3a.access.key': os.environ['AWS_ACCESS_KEY_ID'],
    'spark.hadoop.fs.s3a.secret.key': os.environ['AWS_SECRET_ACCESS_KEY'],
    'spark.driver.memory': os.environ.get('SPARK_DRIVER_MEMORY', '3g'),
}
base = Path('/opt/spark/conf/spark-defaults.conf').read_text()
Path('/tmp/lakehouse-spark.conf').write_text(base + '\n' + '\n'.join(k + ' ' + v for k,v in properties.items()) + '\n')
Path('/tmp/lakehouse-spark.conf').chmod(0o600)
PYTHON
# PyHive selects `default` while opening a connection; create it before accepting clients.
/opt/spark/bin/spark-sql --properties-file /tmp/lakehouse-spark.conf \
  --master 'local[1]' -e 'CREATE NAMESPACE IF NOT EXISTS lakehouse.default;'
exec /opt/spark/bin/spark-submit \
  --properties-file /tmp/lakehouse-spark.conf \
  --master "local[${SPARK_LOCAL_CORES:-2}]" \
  --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
  --name cosmetics-sql-service \
  --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
  --hiveconf hive.server2.thrift.port=10000 \
  --hiveconf hive.server2.authentication=NOSASL
