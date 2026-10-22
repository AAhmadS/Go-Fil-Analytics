# Homework 5 — Scenario Verification Report (live execution, 2026-10-22)

Environment: Docker Compose cluster `hdfs://mycluster` (zookeeper, journalnode,
namenode1/namenode2, datanode1/datanode2, spark) on network `bigdata-net`.
No `cmd/scenarios/` runner exists in this repo (Go prototype removed in Gate 1);
every scenario below was executed directly via container CLI / spark-submit and
the real outputs are quoted verbatim (timestamps trimmed for readability).

Scenario 1: HDFS High Availability & Services Startup
HDFS cluster running with HA support, including two NameNodes (one active, one standby), multiple DataNodes, one JournalNode for failover, ZooKeeper container, separate Spark container on the same Docker network, and Web UI accessible on port 9870.
What we ran: `docker compose ps`; `docker compose exec -T namenode1 hdfs haadmin -getServiceState nn1`; same for `nn2`; HTTP check on `http://localhost:9870/`; `hdfs dfsadmin -report -live`.
Result (exit 0 throughout): all 7 containers `Up (healthy)` on `bigdata-net`; `nn1 -> active`, `nn2 -> standby` ("OK: HA pair is healthy (one active, one standby)"); Web UI `HTTP 200` on `:9870` (and `:9871`); `dfsadmin -report` shows live DataNodes, `Configured Capacity: 2162202353664 (1.97 TB)`, `Missing blocks: 0`, `Under replicated blocks: 0`.

Scenario 2: Data Ingestion to HDFS CLI
Transfer of `yellow_tripdata_<date>.parquet` and `taxi_zone_lookup.csv` into HDFS path `/data/taxi/` via container CLI, ensuring the exact `hdfs://` URI is used with no reliance on the local container filesystem.
What we ran: `bash scripts/upload_to_hdfs.sh` (executes `hdfs dfs -mkdir -p hdfs://mycluster/data/taxi hdfs://mycluster/output`, `hdfs dfs -put -f /workspace/data/yellow_tripdata_2025-10.parquet hdfs://mycluster/data/taxi/`, same for the CSV, then `hdfs dfs -ls hdfs://mycluster/data/taxi/`).
Result (script EXIT 0): `Found 2 items` — `-rw-r--r-- 3 root supergroup 12331 taxi_zone_lookup.csv`, `-rw-r--r-- 3 root supergroup 75267589 yellow_tripdata_2025-10.parquet` (timestamps 2026-10-22 20:04).

Scenario 3: Spark Connectivity to HDFS HA URI
Spark container configured with Java runtime and SparkSession connecting to the logical NameNode URI `hdfs://<logical-namenode-uri>/` for input and output paths.
What we ran: `docker compose exec -T spark spark-submit --master local[*] /tmp/ha_smoke.py` (PySpark session with `spark.hadoop.fs.defaultFS=hdfs://mycluster`, `dfs.nameservices=mycluster`, `dfs.ha.namenodes.mycluster=nn1,nn2`, rpc-addresses for both NameNodes, `ConfiguredFailoverProxyProvider`; then `spark.read.parquet("hdfs://mycluster/data/taxi/yellow_tripdata_*.parquet")` + zone CSV read + counts).
Result (EXIT 0): `fs.defaultFS = hdfs://mycluster`; `trip rows = 4428699`; `trip cols = 20`; `zone rows = 265`; `HA_HANDSHAKE_OK`.

Scenario 4: Analysis 1 (Long Trips with Duration)
Filter trips with `passenger_count > 2` and `trip_distance > 5` miles, compute new column `duration_minutes` (difference between pickup and dropoff in minutes), sort descending by `duration_minutes`, and save as Parquet to `/output/q1_long_trips.parquet`.
What we ran: `bash scripts/run_spark_jobs.sh q1` equivalent via `run_spark_jobs.sh all` → `mvn clean package` + `spark-submit --class com.taxi.analytics.TaxiAnalysisJob --master local[*] spark-analytics-1.0.0.jar all`; then `hdfs dfs -ls hdfs://mycluster/output/q1_long_trips.parquet`.
Result (EXIT 0): `[Q1] input row count = 4428699`, `[Q1] filtered row count = 36180`; preview (top 5 longest) shows `duration_minutes` up to `1439.83` (e.g. VendorID 2, 2025-10-22 pickup, 6 passengers, 21.68 miles, fare 70.0); `hdfs dfs -ls` → `_SUCCESS` (0 B) + `part-00000` (463,994 B) + `part-00001` (576,120 B); `[Q1] wrote hdfs://mycluster/output/q1_long_trips.parquet`.

Scenario 5: Analysis 2 (Average Fare per Zone)
Join trip data with `taxi_zone_lookup.csv` on `PULocationID = LocationID`, group by `Zone`, compute average `fare_amount`, output `Zone`, `Borough`, and average fare, and save as Parquet to `/output/q2_avg_fare_by_zone.parquet`.
What we ran: same `spark-submit` run (STEP 2); then `hdfs dfs -ls hdfs://mycluster/output/q2_avg_fare_by_zone.parquet`.
Result (EXIT 0): `[Q2] groups = 261` over 265 zones; preview `|Zone|Borough|avg_fare_amount|` includes `|East Village|Manhattan|13.6008|`, `|Whitestone|Queens|25.3032|`; `hdfs dfs -ls` → `_SUCCESS` (0 B) + `part-00000` (6,690 B); wrote `hdfs://mycluster/output/q2_avg_fare_by_zone.parquet`.

Scenario 6: Analysis 3 (Revenue and Trip Count by Destination Borough)
Join trip data with `taxi_zone_lookup.csv` on `DOLocationID = LocationID`, group by `Borough`, compute sum of `total_amount` and total trip count, and save as Parquet to `/output/q3_revenue_by_borough.parquet`.
What we ran: same `spark-submit` run (STEP 3); then `hdfs dfs -ls hdfs://mycluster/output/q3_revenue_by_borough.parquet`.
Result (EXIT 0): `[Q3] boroughs = 8`; preview `|Borough|total_revenue|trip_count|` includes `|Queens|1.14005E7|247883|`, `|Brooklyn|9652736.79|254362|`, `|Staten Island|98953.17|1249|`; `hdfs dfs -ls` → `_SUCCESS` (0 B) + `part-00000` (1,207 B); wrote `hdfs://mycluster/output/q3_revenue_by_borough.parquet`.

Scenario 7: Analysis 4 (Maximum Tip Amount per Day)
Extract date from pickup timestamp, group by date, compute maximum `tip_amount`, output date and max tip, and save as Parquet to `/output/q4_max_tip_per_day.parquet`.
What we ran: same `spark-submit` run (STEP 4, `=== [DONE] requested steps [q1, q2, q3, q4] finished`); then `hdfs dfs -ls hdfs://mycluster/output/q4_max_tip_per_day.parquet`.
Result (EXIT 0): `[Q4] days = 33`; preview `|trip_date|max_tip_amount|` includes `|2025-09-30|4.27|`, `|2025-10-01|110.0|`, `|2025-10-02|90.0|`; `hdfs dfs -ls` → `_SUCCESS` (0 B) + `part-00000` (1,103 B); wrote `hdfs://mycluster/output/q4_max_tip_per_day.parquet`.
