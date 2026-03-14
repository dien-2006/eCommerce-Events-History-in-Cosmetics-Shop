#!/usr/bin/env bash
set -euo pipefail

mkdir -p spark/jars

ICEBERG_VER="1.6.0"
HADOOP_AWS_VER="3.3.6"
AWS_BUNDLE_VER="1.12.262"

base="https://repo1.maven.org/maven2"

declare -a urls=(
  "${base}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VER}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VER}.jar"
  "${base}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VER}/hadoop-aws-${HADOOP_AWS_VER}.jar"
  "${base}/com/amazonaws/aws-java-sdk-bundle/${AWS_BUNDLE_VER}/aws-java-sdk-bundle-${AWS_BUNDLE_VER}.jar"
)

echo "[jars] Downloading to spark/jars/"
for u in "${urls[@]}"; do
  f="spark/jars/$(basename "$u")"
  if [[ -f "$f" ]]; then
    echo " - exists: $f"
    continue
  fi
  echo " - $u"
  curl -L --fail -o "$f" "$u"
done

echo "[jars] Done."
ls -lh spark/jars