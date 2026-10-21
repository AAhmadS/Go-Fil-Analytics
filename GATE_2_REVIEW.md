# GATE 2 REVIEW — NYC Taxi Data Preparation + HDFS CLI Upload

Branch: `gate-2-data-ingestion` (branched from `gate-1-infrastructure`).

## 1. Files added / modified in this gate
| Path | Status | Purpose |
|---|---|---|
| `scripts/prepare_data.sh` | new | Ensures `./data/yellow_tripdata_*.parquet` + `./data/taxi_zone_lookup.csv` exist: keeps local files, else `curl`s official NYC TLC URLs, else runs the offline fallback generator. |
| `scripts/generate_fallback_data.py` | new | Deterministic (seed 42) 1,000-row TLC-schema parquet writer + zone CSV writer. Uses **pyspark inside the `spark` container** when Docker is up (100% offline), else **pyarrow on the host**. Covers Manhattan/Queens/Brooklyn/Bronx/Staten Island + JFK/LaGuardia. |
| `scripts/upload_to_hdfs.sh` | new | Pure container-CLI upload: `docker compose exec namenode1 hdfs dfs -mkdir -p hdfs://mycluster/data/taxi /output`, `-put -f` both inputs, final `-ls` proof. Never touches HDFS from the host. |
| `data/taxi_zone_lookup.csv` | kept | Real TLC zone file (from Gate 1, now canonical under `data/`). |
| `data/yellow_tripdata_*.parquet` | gitignored | Large binaries excluded via `.gitignore` (`data/yellow_tripdata_*.parquet`); `prepare_data.sh` recreates a valid sample anywhere. |

## 2. OS-concepts mapping
- **`hdfs dfs -put` ≈ `cp` from local disk to a networked block device.** The CLI streams the file through the active NameNode (metadata/inode update) while blocks land on DataNodes — like writing through a VFS layer.
- **`-mkdir -p hdfs://mycluster/...` ≈ `mkdir -p` on a distributed inode tree.** Only metadata is created; no blocks until data arrives.
- **Download-then-fallback ≈ page-cache miss → swap-in path.** Fast path (local file), slow path (network fetch), guaranteed path (synthetic generation) — the pipeline can never deadlock on network policy.
- **Shared `hdfs://mycluster` URI ≈ virtual mount point.** Uploader, Spark, and verifier all address the same logical filesystem; ZooKeeper failover keeps the mount alive across NameNode switches.

## 3. Human verification — up to this gate (assumes Gate 1 cluster is up)
```bash
# 1) prepare inputs (works offline: falls back to 1000-row generator)
bash scripts/prepare_data.sh
ls -lh data/   # expect taxi_zone_lookup.csv + yellow_tripdata_*.parquet

# 2) upload via container CLI
bash scripts/upload_to_hdfs.sh

# 3) proof the files live in HDFS (not just locally)
docker compose exec namenode1 hdfs dfs -ls hdfs://mycluster/data/taxi/
# expect: yellow_tripdata_*.parquet + taxi_zone_lookup.csv
```
