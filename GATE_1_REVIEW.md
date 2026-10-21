# GATE 1 REVIEW — HDFS HA Infrastructure + Docker Network + Bootstrap Automation

Branch: `gate-1-infrastructure` (branched from initial `main`).

## 1. Files added / modified in this gate
| Path | Status | Purpose |
|---|---|---|
| `docker-compose.yml` | rewritten | 7 containers on single bridge `bigdata-net`: `zookeeper` (official `zookeeper:3.9`), `journalnode` (journalnode daemon run from the `bde2020/hadoop-namenode` image — bde2020 ships no journalnode image), `namenode1` (host `9870:9870`), `namenode2` (host `9871:9870`), `datanode1`, `datanode2`, `spark` (built from `./spark/Dockerfile`). Logical URI everywhere is `hdfs://mycluster`. Hadoop daemons generate their XML from env vars (`___` → `-` per bde2020 convention); old `ns1`/3-journalnode/`hadoopnet` layout and all Go files were removed. |
| `hadoop-conf/core-site.xml` | rewritten | `fs.defaultFS=hdfs://mycluster`, `ha.zookeeper.quorum=zookeeper:2181`. Mounted into namenodes and `spark:/opt/hadoop/etc/hadoop` so Spark resolves `hdfs://mycluster/` natively. |
| `hadoop-conf/hdfs-site.xml` | rewritten | `dfs.nameservices=mycluster`, `dfs.ha.namenodes.mycluster=nn1,nn2`, RPC `namenode1:8020`/`namenode2:8020`, HTTP `namenode1:9870`/`namenode2:9870`, `shared.edits.dir=qjournal://journalnode:8485/mycluster`, `ConfiguredFailoverProxyProvider`, `automatic-failover=true`, fencing `shell(/bin/true)`, replication 2. Mounted read-only into `spark:/opt/hadoop/etc/hadoop` (Hadoop daemons instead render the identical XML from env vars, since the bde2020 entrypoint rewrites `/etc/hadoop/*-site.xml` at startup). |
| `spark/Dockerfile` | new | `FROM apache/spark:3.5.2` + `maven` + `curl` + `python3` (official image — Bitnami images were delisted from Docker Hub); sets `HDFS_URI`, `HADOOP_CONF_DIR`. Gives the Spark container JDK 11 + Maven + Spark on the same `bigdata-net`. |
| `bootstrap_cluster.sh` | new | Deterministic idempotent HA bring-up (8 steps, see §3). Safe to re-run. |
| `verify_gate1.sh` | new | Checks `haadmin -getServiceState nn1/nn2` = active+standby, `curl localhost:9870`, `dfsadmin -report`. Exit 0 = PASS. |
| `data/` | reorganised | `taxi_zone_lookup.csv` moved to `data/`; large `*.parquet` files are `.gitignore`d (never commit 75 MB binaries). |
| removed | deleted | Legacy Go pipeline (`main.go`, `go.mod/sum`, `queries/`, `models/`, `storage/`, old `Dockerfile`) and stale docs — spec is Java-only. |

## 2. OS-concepts mapping (for the presentation)
- **NameNode ≈ inode table / file-system metadata server.** Holds the namespace (file → block list). HA runs two copies: one `active` (serves writes), one `standby` (replays edits, hot spare).
- **DataNode ≈ disk blocks / block store.** Stores actual block replicas (`dfs.replication=2` across `datanode1/2`) and heartbeats to the active NameNode.
- **JournalNode ≈ Write-Ahead Log (WAL).** `qjournal://journalnode:8485/mycluster` is the shared edit log both NameNodes agree on — the single source of truth that makes failover safe.
- **ZooKeeper ≈ distributed mutex + heartbeat monitor / failure detector.** Holds the active-standby lock (`ha.zookeeper.quorum`), runs leader election, and triggers automatic failover via `zkfc` daemons on each NameNode (compare: `etcd`/Raft leader election, fencing like STONITH).
- **Docker bridge `bigdata-net` ≈ virtual LAN / single rack.** All 7 containers share one private subnet with DNS names (`namenode1`, `journalnode`, …); only `9870/9871/2181/8485/8020` are published to the host.
- **`bootstrap_cluster.sh` ≈ OS boot sequence (`init`).** Ordered, idempotent bring-up: storage daemons first (ZK/journal), then metadata format (like `mkfs`), then metadata servers, then data servers — exactly like mounting WAL before the filesystem.

## 3. Bootstrap order (what the script does)
1. `up -d zookeeper journalnode` (quorum + WAL first).
2. `up -d namenode1` — the bde2020 `run.sh` self-formats the empty name volume once, then starts the NameNode.
3. `hdfs zkfc -formatZK` once (`|| true` if already formatted) + start `zkfc` on nn1.
4. One-off `docker compose run --rm namenode2 hdfs namenode -bootstrapStandby` (copies fsimage; skipped when `VERSION` exists), so the real `namenode2` service never self-formats.
5. `up -d namenode2` + start its `zkfc` (standby replays journal).
6. `up -d datanode1 datanode2` (block workers register with active NN).
7. `up -d --build spark` (client; shares `hadoop-conf`, resolves `hdfs://mycluster`).

## 4. Human verification — ONLY up to this gate
```bash
# from the repo root
docker compose down -v
./bootstrap_cluster.sh
# wait ~60s for election, then:
./verify_gate1.sh
docker compose exec namenode1 hdfs haadmin -getServiceState nn1
docker compose exec namenode1 hdfs haadmin -getServiceState nn2
# expect one "active" and one "standby"
curl -I http://localhost:9870   # namenode1 Web UI
curl -I http://localhost:9871   # namenode2 Web UI
docker compose ps                # 7 containers, all running
```
Do NOT expect `/data/taxi` or `/output` yet — those arrive in Gates 2–3.
