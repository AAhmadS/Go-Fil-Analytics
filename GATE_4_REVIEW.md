# GATE 4 REVIEW — Deliverables, End-to-End Verification & Video Guide

Branch: `gate-4-final-submission` (branched from `gate-3-spark-analytics`).

## 1. Files added in this gate
| Path | Purpose |
|---|---|
| `DOCUMENTATION.md` | Official homework deliverable: exact copy-paste commands for (1) cluster start, (2) HDFS CLI upload, (3) Spark jobs from the Spark container, (4) exact `hdfs://mycluster/...` input/output URIs. |
| `VIDEO_WALKTHROUGH_GUIDE.md` | Live screen-recording teleprompter: Video 1 (bring-up, `haadmin` states, Web UI `:9870`, CLI upload, `ls /data/taxi`), Video 2 (`spark-submit`, narrating `.show(5)` Q1–Q4 previews, `ls /output`). |
| `scripts/smoke_test.sh` | One-command E2E: bootstrap → verify → prepare/upload → `run_spark_jobs.sh all` → assert all 4 parquet outputs exist with `_SUCCESS`. Exit 0 = PASS. |
| `GATE_4_REVIEW.md` | This file. |

## 2. OS-concepts mapping
- **`DOCUMENTATION.md` ≈ runbook / SOP.** The human-readable equivalent of the bootstrap script: deterministic recovery procedure, like `/usr/share/doc` + `systemd` unit docs.
- **`smoke_test.sh` ≈ CI pipeline / integration test.** Builds the world, runs the workload, asserts postconditions — the same shape as a post-deploy health gate.
- **Video guide ≈ flight-recorder narration.** Timestamps + expected outputs per scene, so a reviewer can diff the recording against the script like replaying a log.

## 3. E2E validation actually performed (this machine, 2026-10-21)
- `./bootstrap_cluster.sh` → EXIT 0 (after fixes below); `./verify_gate1.sh` → `VERIFY_GATE1: PASS` (`nn1=standby, nn2=active`, both Web UIs 200, 2 live DataNodes).
- `bash scripts/prepare_data.sh` → kept local 75 MB `yellow_tripdata_2025-10.parquet` + real zone CSV; fallback generator separately tested → 1,000 rows, exact TLC schema.
- `bash scripts/upload_to_hdfs.sh` → EXIT 0; `ls hdfs://mycluster/data/taxi/` shows both files.
- `bash scripts/run_spark_jobs.sh all` → EXIT 0; 4.43M input rows; Q1 36,180 rows, Q2 261 zone groups, Q3 8 boroughs, Q4 33 days; all 4 `hdfs://mycluster/output/q*.parquet` present with `_SUCCESS` + snappy parts.
- `bash scripts/smoke_test.sh` → `SMOKE_TEST: PASS`.

## 4. Bugs found during live E2E and fixed (all committed on their home gates)
1. `bitnami/spark:3.5.2` / `bitnami/zookeeper:3.9` no longer exist on Docker Hub (Bitnami delisted) → `apache/spark:3.5.2` + official `zookeeper:3.9`.
2. `bde2020/hadoop-journalnode` never existed → journalnode daemon runs from the `bde2020/hadoop-namenode` image with `command: ["hdfs", "journalnode"]`.
3. `bash -lc "hdfs ..."` fails inside bde2020 containers (Debian `/etc/profile` resets root PATH, hiding `/opt/hadoop-*/bin`) → all Hadoop/Spark invocations run directly via `docker compose exec`, using `exec -w` instead of `cd`.
4. `bootstrapStandby` in a `compose run` one-off container cannot infer its NN id (random hostname) and rejects CLI `-D` flags → injected `HDFS_CONF_dfs_ha_namenode_id=nn2` (rendered into `hdfs-site.xml` by the entrypoint); exit 5 (already bootstrapped) tolerated.
5. `verify_gate1.sh` false FAIL: `pipefail` + `| head` trips SIGPIPE → dropped `pipefail`, kept explicit error checks.
6. JournalNode inherited the namenode `curl :9870` HEALTHCHECK → always `unhealthy` → overridden with `nc -z localhost 8485` (dash has no `/dev/tcp`).
7. Apache Spark image ships a JRE, `mvn package` needs a JDK → `openjdk-11-jdk-headless` + `JAVA_HOME` in `spark/Dockerfile`.
8. Git Bash (MSYS2) rewrites `/workspace/...` args to `C:\...` for native `docker.exe` (`put: No FileSystem for scheme "C"`) → `export MSYS_NO_PATHCONV=1` in every script.
9. Operational lesson: with a single JournalNode, **never restart it while the cluster runs** — the active NameNode aborts on journal-flush failure (observed twice, recovered via automatic failover + `up -d <namenode>`). Reviewers should treat `down -v` + fresh bootstrap as the reset path, not rolling restarts.

## 5. Human verification — full stack
```bash
bash scripts/smoke_test.sh   # ~10 min first run; ends with SMOKE_TEST: PASS
# or step by step from DOCUMENTATION.md sections 1-4.
```
