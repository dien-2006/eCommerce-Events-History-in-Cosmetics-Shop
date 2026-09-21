#!/usr/bin/env bash
set -euo pipefail
jars_dir="${1:-spark/jars}"
mkdir -p "$jars_dir"
# Spark 3.5.7 uses Hadoop 3.3.4. S3A uses AWS SDK v1; Iceberg S3FileIO uses v2.
base=https://repo.maven.apache.org/maven2
checksum_file="$(dirname "$0")/jars.sha256"
artifacts=(
  org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.0/iceberg-spark-runtime-3.5_2.12-1.10.0.jar
  org/apache/iceberg/iceberg-aws-bundle/1.10.0/iceberg-aws-bundle-1.10.0.jar
  org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
  com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
  org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar
)
for artifact in "${artifacts[@]}"; do
  name="${artifact##*/}"
  expected="$(awk -v jar="$name" '$2 == jar {print $1}' "$checksum_file")"
  [[ -n "$expected" ]] || { echo "Missing pinned checksum: $name" >&2; exit 1; }
  if [[ ! -f "$jars_dir/$name" ]]; then
    curl -fsSL --retry 4 "$base/$artifact" -o "$jars_dir/$name"
  fi
  actual="$(sha256sum "$jars_dir/$name")"
  [[ "${actual%% *}" == "$expected" ]] || { echo "Checksum failed: $name" >&2; exit 1; }
done
