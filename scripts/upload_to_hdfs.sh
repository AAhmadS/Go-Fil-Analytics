#!/usr/bin/env bash
# Gate 2 — step 2: upload both inputs to HDFS via container CLI only.
# Target: hdfs://mycluster/data/taxi/
# NOTE: hdfs runs directly via exec (no `bash -lc` wrapper — see bootstrap).
set -euo pipefail
export MSYS_NO_PATHCONV=1
# Git Bash (MSYS2) rewrites leading-slash args to C:\... for native docker.exe
# — disabled so container paths and hdfs:// URIs pass through untouched.
cd "$(dirname "$0")/.."

echo "=== [UPLOAD 1/4] mkdir hdfs://mycluster/data/taxi + /output ==="
docker compose exec -T namenode1 hdfs dfs -mkdir -p hdfs://mycluster/data/taxi hdfs://mycluster/output

echo "=== [UPLOAD 2/4] put yellow_tripdata_*.parquet ==="
for f in data/yellow_tripdata_*.parquet; do
  base="$(basename "$f")"
  echo "-> /workspace/data/$base to hdfs://mycluster/data/taxi/$base"
  docker compose exec -T namenode1 hdfs dfs -put -f "/workspace/data/$base" "hdfs://mycluster/data/taxi/$base"
done

echo "=== [UPLOAD 3/4] put taxi_zone_lookup.csv ==="
docker compose exec -T namenode1 hdfs dfs -put -f /workspace/data/taxi_zone_lookup.csv hdfs://mycluster/data/taxi/taxi_zone_lookup.csv

echo "=== [UPLOAD 4/4] proof: hdfs dfs -ls hdfs://mycluster/data/taxi/ ==="
docker compose exec -T namenode1 hdfs dfs -ls hdfs://mycluster/data/taxi/
