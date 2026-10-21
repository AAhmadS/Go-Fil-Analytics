# VIDEO_WALKTHROUGH_GUIDE — Live Screen-Recording Teleprompter

Two videos, both recorded as live terminal sessions. Keep a browser tab with
`http://localhost:9870` open for cutaways. Read the **SAY** lines aloud while
typing the **TYPE** lines.

---

## VIDEO 1 — Cluster bring-up + HDFS upload (~5 min)

### V1 Scene 1 — Cluster up (0:00–1:30)
**SAY:** "Homework 5, video 1. I start the full HDFS high-availability stack —
two NameNodes, two DataNodes, one JournalNode, ZooKeeper, and a Spark client —
on one Docker bridge network called bigdata-net."
**TYPE:**
```bash
docker compose down -v
./bootstrap_cluster.sh
```
**SAY (while it runs):** "The bootstrap is ordered like an OS boot: ZooKeeper
and the JournalNode first — that is our distributed lock and write-ahead log —
then we format NameNode 1 once, format the ZooKeeper failover state once,
start NameNode 1 with its failover controller, bootstrap NameNode 2 from it,
start it, then the DataNodes and Spark."

### V1 Scene 2 — HA proof (1:30–2:30)
**TYPE:**
```bash
docker compose ps
docker compose exec namenode1 hdfs haadmin -getServiceState nn1
docker compose exec namenode1 hdfs haadmin -getServiceState nn2
```
**SAY:** "Seven containers, all running. nn1 is active, nn2 is standby — that
is our hot spare. The NameNode is the inode table, DataNodes are disk blocks,
the JournalNode is the write-ahead log, ZooKeeper is the heartbeat monitor."
**CUTAWAY:** refresh `http://localhost:9870`, show Overview + DataNodes (2 live).

### V1 Scene 3 — Data upload (2:30–4:30)
**TYPE:**
```bash
bash scripts/prepare_data.sh
ls -lh data/
bash scripts/upload_to_hdfs.sh
docker compose exec namenode1 hdfs dfs -ls hdfs://mycluster/data/taxi/
```
**SAY:** "Prepare keeps local files, otherwise downloads the official TLC
sample, otherwise generates a valid 1,000-row fallback offline via Spark — so
this never fails on network policy. Upload uses only container CLI: mkdir the
inode path hdfs://mycluster/data/taxi, put both files, list to prove they are
in HDFS, not just on my laptop."

### V1 Outro (4:30–5:00)
**SAY:** "Video 1 done: cluster green, HA pair elected, both inputs in
hdfs://mycluster/data/taxi. Video 2 runs the four Spark analyses."

---

## VIDEO 2 — Spark jobs + output verification (~6 min)

### V2 Scene 1 — Build + submit (0:00–2:30)
**SAY:** "Video 2. The Spark container has JDK, Maven and Spark on the same
Docker network and mounts our Hadoop configs, so hdfs://mycluster resolves
natively. Every path in code is a full hdfs:// URI — nothing touches local
disk."
**TYPE:**
```bash
bash scripts/run_spark_jobs.sh all
```
**SAY (while Maven/Spark runs):** "Watch the banners: STEP 1 long trips with
3+ passengers over 5 miles and a duration column, STEP 2 average fare per
pickup zone, STEP 3 revenue per dropoff borough, STEP 4 max tip per day. Each
prints row counts, schema, and a 5-row preview before writing parquet."

### V2 Scene 2 — Narrate Q1–Q4 previews (2:30–4:30)
Point at each `=== [STEP N] ===` block and `.show(5, false)` table as it flies
by. Suggested lines:
- Q1: "Filtered long group trips, ordered by duration descending."
- Q2: "Trips joined to zones on PULocationID, averaged fare per Zone/Borough."
- Q3: "Dropoff join, revenue is sum of total_amount plus trip counts."
- Q4: "Calendar date from pickup timestamp, max tip per day."

### V2 Scene 3 — Output proof (4:30–6:00)
**TYPE:**
```bash
docker compose exec namenode1 hdfs dfs -ls hdfs://mycluster/output/
docker compose exec namenode1 hdfs dfs -ls hdfs://mycluster/output/q1_long_trips.parquet
docker compose exec namenode1 hdfs dfs -ls hdfs://mycluster/output/q2_avg_fare_by_zone.parquet
docker compose exec namenode1 hdfs dfs -ls hdfs://mycluster/output/q3_revenue_by_borough.parquet
docker compose exec namenode1 hdfs dfs -ls hdfs://mycluster/output/q4_max_tip_per_day.parquet
```
**SAY:** "All four parquet outputs exist under hdfs://mycluster/output with
_SUCCESS markers and part files — q1 through q4. That closes the loop: Docker
network, HA filesystem, container-CLI ingest, Java Dataset API analytics,
verified outputs. Thank you."
