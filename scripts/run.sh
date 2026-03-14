# 1) Configure env
cp .env.example .env

# 2) Download required Spark jars
bash scripts/download_jars.sh

# 3) Start stack
docker compose up -d --build

# 4) Verify everything is up
bash scripts/verify_services.sh

# 5) (Optional) Iceberg smoke test (Spark SQL)
docker exec -it spark-master /opt/spark/bin/spark-sql \
  -f /opt/spark/jobs/../conf/../.. >/dev/null 2>&1 || true

# 6) Ingest Bronze (put Kaggle CSV under ./data first)
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.0.jar,/opt/spark/jars/hadoop-aws-3.3.6.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
  /opt/spark/jobs/bronze_ingest.py \
  --input /data \
  --catalog rest \
  --db cosmetics_bronze \
  --table events_history \
  --batch-id batch_001

# 7) Bronze verification
docker exec -it spark-master /opt/spark/bin/spark-sql \
  -e "SELECT count(*) FROM rest.cosmetics_bronze.events_history;"

# 8) ClickHouse ping
curl -fsS "http://localhost:8123/ping" && echo

# 9) Superset URL
echo "Superset: http://localhost:8088 (admin/admin)"