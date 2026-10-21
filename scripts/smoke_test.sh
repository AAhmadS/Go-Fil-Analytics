#!/usr/bin/env bash
# Gate 4 — end-to-end smoke test: cluster -> data -> spark -> 4 outputs.
# Fails (exit 1) if any of the 4 parquet outputs is missing or empty.
set -euo pipefail
export MSYS_NO_PATHCONV=1
# Git Bash (MSYS2) rewrites leading-slash args to C:\... for native docker.exe - disable so container paths and hdfs:// URIs pass through untouched.
cd "$(dirname "$0")/.."

echo "########## SMOKE 1/5 — bootstrap cluster ##########"
./bootstrap_cluster.sh

echo "########## SMOKE 1.5/5 — settle ZooKeeper election ##########"
echo "Automatic failover needs ~60s after (re)bootstrap before the Web UIs"
echo "answer reliably; wait it out instead of racing it."
sleep 60

echo "########## SMOKE 2/5 — verify HA ##########"
./verify_gate1.sh

echo "########## SMOKE 3/5 — prepare + upload data ##########"
bash scripts/prepare_data.sh
bash scripts/upload_to_hdfs.sh

echo "########## SMOKE 4/5 — run Spark jobs (all) ##########"
bash scripts/run_spark_jobs.sh all

echo "########## SMOKE 5/5 — assert 4 parquet outputs ##########"
FAIL=0
for q in q1_long_trips q2_avg_fare_by_zone q3_revenue_by_borough q4_max_tip_per_day; do
  echo "--- hdfs://mycluster/output/$q.parquet ---"
  if docker compose exec -T namenode1 hdfs dfs -test -e "hdfs://mycluster/output/$q.parquet/_SUCCESS" \
    && docker compose exec -T namenode1 hdfs dfs -du -s "hdfs://mycluster/output/$q.parquet"; then
    docker compose exec -T namenode1 hdfs dfs -ls "hdfs://mycluster/output/$q.parquet"
  else
    echo "FAIL: missing or incomplete output $q"
    FAIL=1
  fi
done
if [ "$FAIL" = "1" ]; then echo "SMOKE_TEST: FAIL"; exit 1; fi
echo "SMOKE_TEST: PASS — all 4 outputs present with _SUCCESS markers."
