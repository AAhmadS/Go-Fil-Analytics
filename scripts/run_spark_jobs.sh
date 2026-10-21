#!/usr/bin/env bash
# Gate 3 — build the Maven project and spark-submit inside the spark container.
# Usage: bash scripts/run_spark_jobs.sh [q1|q2|q3|q4|all|q1,q2,...]  (default: all)
#
# NOTE: commands run directly (no `bash -lc` wrapper): Ubuntu's /etc/profile
# resets PATH for root login shells, which would hide /opt/spark/bin.
# `docker compose exec -w` sets the working directory instead of `cd`.
set -euo pipefail
export MSYS_NO_PATHCONV=1
# Git Bash (MSYS2) rewrites leading-slash args to C:\... for native docker.exe
# — disabled so container paths and hdfs:// URIs pass through untouched.
cd "$(dirname "$0")/.."

STEPS="${1:-all}"
JAR="/workspace/spark-app/target/spark-analytics-1.0.0.jar"

echo "=== [SPARK 1/3] mvn clean package (inside spark container) ==="
docker compose exec -T -w /workspace/spark-app spark mvn -q -DskipTests clean package

echo "=== [SPARK 2/3] spark-submit TaxiAnalysisJob [$STEPS] ==="
docker compose exec -T spark spark-submit --class com.taxi.analytics.TaxiAnalysisJob --master local[*] "$JAR" "$STEPS"

echo "=== [SPARK 3/3] proof: hdfs dfs -ls hdfs://mycluster/output/ ==="
docker compose exec -T namenode1 hdfs dfs -ls hdfs://mycluster/output/
for q in q1_long_trips q2_avg_fare_by_zone q3_revenue_by_borough q4_max_tip_per_day; do
  docker compose exec -T namenode1 hdfs dfs -ls "hdfs://mycluster/output/$q.parquet"
done
