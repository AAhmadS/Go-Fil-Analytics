# GATE 3 REVIEW — Java Spark Dataset API Analytics (Q1–Q4)

Branch: `gate-3-spark-analytics` (branched from `gate-2-data-ingestion`).

## 1. Files added / modified in this gate
| Path | Status | Purpose |
|---|---|---|
| `spark-app/pom.xml` | new | Maven project (`com.taxi:spark-analytics:1.0.0`, Java 17, `spark-sql_2.12:3.5.2` `provided` to match the `bitnami/spark:3.5.2` runtime; `Main-Class: com.taxi.analytics.TaxiAnalysisJob`). Built with `mvn clean package` **inside** the spark container. |
| `spark-app/src/main/java/com/taxi/analytics/TaxiAnalysisJob.java` | new | Single Dataset<Row> app, `spark-submit … [q1\|q2\|q3\|q4\|all]`. HA via explicit `spark.hadoop.*` keys + mounted `hadoop-conf`. Every path is a full `hdfs://mycluster/…` URI. Each query prints banner, counts, schema, `.show(5, false)` preview before `SaveMode.Overwrite` parquet write. |
| `scripts/run_spark_jobs.sh` | new | `mvn clean package` + `spark-submit --class … --master local[*]` in `spark` container, then `hdfs dfs -ls` proof for all 4 outputs. |
| Analyses | implemented | **Q1** filter `passenger_count>2 AND trip_distance>5` + `duration_minutes=(unix(dropoff)-unix(pickup))/60.0`, `desc`, → `q1_long_trips.parquet`. **Q2** join `PULocationID=LocationID`, group `Zone,Borough`, `avg(fare_amount)→avg_fare_amount` → `q2_…`. **Q3** join `DOLocationID=LocationID`, group `Borough`, `sum(total_amount)→total_revenue`, `count→trip_count` → `q3_…`. **Q4** `to_date(pickup)→trip_date`, group, `max(tip_amount)→max_tip_amount` → `q4_…`. |

## 2. OS-concepts mapping
- **SparkSession ≈ process + mount table.** `spark.hadoop.fs.defaultFS/conf` is the mount entry that binds the process to the distributed filesystem, like mounting NFS before `open()`.
- **Dataset<Row> ≈ typed in-memory page cache / materialised view.** Lazy transformations build a query plan (like a syscall batch); `.count()/.show()/.write()` trigger execution (like `fsync`).
- **Join on LocationID ≈ foreign-key join between fact table (trips) and dimension table (zones)** — the classic star-schema pattern from databases.
- **`.show(5,false)` + banners ≈ `printf` tracing / `dmesg`.** Deliberate observability so a screen recording can narrate every stage, mirroring kernel log levels.
- **`SaveMode.Overwrite` ≈ O_TRUNC open flag.** Re-running a query idempotently replaces the output directory, safe for repeated presentations.

## 3. Human verification — up to this gate (cluster + data from Gates 1–2 required)
```bash
./bootstrap_cluster.sh            # if cluster is down
bash scripts/prepare_data.sh     # inputs exist locally
bash scripts/upload_to_hdfs.sh   # inputs in hdfs://mycluster/data/taxi/
# build + run all four analyses (takes several minutes first time: maven pull)
bash scripts/run_spark_jobs.sh all
# individual queries:
bash scripts/run_spark_jobs.sh q1
# proof:
docker compose exec namenode1 hdfs dfs -ls hdfs://mycluster/output/
```
