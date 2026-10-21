# Homework 5 — Official Deliverable: Exact Command Runbook

All commands run from the repository root. The logical HDFS URI used
everywhere is **`hdfs://mycluster`**.

## 1. Start the HDFS HA + Spark cluster (Docker Compose)

```bash
docker compose down -v
./bootstrap_cluster.sh
# wait ~60s for ZooKeeper election, then verify:
./verify_gate1.sh
docker compose exec namenode1 hdfs haadmin -getServiceState nn1
docker compose exec namenode1 hdfs haadmin -getServiceState nn2
# expect one "active" and one "standby"
docker compose ps
```

Web UIs: `http://localhost:9870` (namenode1), `http://localhost:9871` (namenode2).

## 2. Upload the input files to `hdfs://mycluster/data/taxi/` (container CLI)

```bash
bash scripts/prepare_data.sh
bash scripts/upload_to_hdfs.sh
docker compose exec namenode1 hdfs dfs -ls hdfs://mycluster/data/taxi/
```

What `upload_to_hdfs.sh` executes inside the `namenode1` container:

```bash
hdfs dfs -mkdir -p hdfs://mycluster/data/taxi hdfs://mycluster/output
hdfs dfs -put -f /workspace/data/yellow_tripdata_*.parquet hdfs://mycluster/data/taxi/
hdfs dfs -put -f /workspace/data/taxi_zone_lookup.csv hdfs://mycluster/data/taxi/
hdfs dfs -ls hdfs://mycluster/data/taxi/
```

## 3. Run the Spark jobs from inside the Spark container

```bash
bash scripts/run_spark_jobs.sh all
# or individually:
bash scripts/run_spark_jobs.sh q1
bash scripts/run_spark_jobs.sh q2
bash scripts/run_spark_jobs.sh q3
bash scripts/run_spark_jobs.sh q4
```

Equivalent manual form (what the script wraps):

```bash
docker compose exec -w /workspace/spark-app spark mvn -q -DskipTests clean package
docker compose exec spark spark-submit --class com.taxi.analytics.TaxiAnalysisJob --master local[*] /workspace/spark-app/target/spark-analytics-1.0.0.jar all
```

## 4. Exact HDFS input and output URIs

Inputs (read by every query):

- `hdfs://mycluster/data/taxi/yellow_tripdata_*.parquet`
- `hdfs://mycluster/data/taxi/taxi_zone_lookup.csv`

Outputs (one per query, `SaveMode.Overwrite` parquet):

- `hdfs://mycluster/output/q1_long_trips.parquet`
- `hdfs://mycluster/output/q2_avg_fare_by_zone.parquet`
- `hdfs://mycluster/output/q3_revenue_by_borough.parquet`
- `hdfs://mycluster/output/q4_max_tip_per_day.parquet`

Verify:

```bash
docker compose exec namenode1 hdfs dfs -ls hdfs://mycluster/output/
docker compose exec namenode1 hdfs dfs -ls hdfs://mycluster/output/q1_long_trips.parquet
docker compose exec namenode1 hdfs dfs -ls hdfs://mycluster/output/q2_avg_fare_by_zone.parquet
docker compose exec namenode1 hdfs dfs -ls hdfs://mycluster/output/q3_revenue_by_borough.parquet
docker compose exec namenode1 hdfs dfs -ls hdfs://mycluster/output/q4_max_tip_per_day.parquet
```

One-command end-to-end smoke test: `bash scripts/smoke_test.sh`.
