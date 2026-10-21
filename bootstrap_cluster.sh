#!/usr/bin/env bash
# Deterministic, idempotent HDFS HA bootstrap for hdfs://mycluster.
# Order: zookeeper + journalnode -> namenode1 (self-formats on first up) ->
# zkfc-format + zkfc -> bootstrapStandby nn2 (one-off) -> nn2 + zkfc ->
# datanodes + spark.
#
# NOTE: never wrap hdfs commands in `bash -lc` on the bde2020 images —
# Debian's /etc/profile resets PATH for root login shells, dropping
# /opt/hadoop-*-*/bin. Direct `docker compose exec <svc> hdfs ...` inherits
# the image ENV (PATH + HADOOP_CONF_DIR) and works.
set -euo pipefail
export MSYS_NO_PATHCONV=1
# Git Bash (MSYS2) rewrites leading-slash args to C:\... for native docker.exe
# — disabled so container paths and hdfs:// URIs pass through untouched.
cd "$(dirname "$0")"

poll_ui() { # poll_ui <url> <tries>
  local url="$1" tries="${2:-60}"
  for ((i=1; i<=tries; i++)); do
    # --noproxy: never route localhost checks through a (possibly flaky)
    # HTTP_PROXY; the target is always the local Docker Desktop port forward.
    if curl -sf -m 5 --noproxy '*' "$url" -o /dev/null 2>/dev/null; then return 0; fi
    sleep 5
  done
  echo "WARN: $url never came up" >&2
  return 0
}

echo "=== [BOOTSTRAP 1/7] Starting zookeeper + journalnode ==="
docker compose up -d zookeeper journalnode
sleep 15
docker compose ps

echo "=== [BOOTSTRAP 2/7] Starting namenode1 (self-formats empty volume) ==="
docker compose up -d namenode1
poll_ui http://localhost:9870/jmx 60
docker compose exec -T namenode1 hdfs dfsadmin -safemode wait || true

echo "=== [BOOTSTRAP 3/7] Formatting ZooKeeper HA state + starting zkfc on nn1 ==="
docker compose exec -T namenode1 hdfs zkfc -formatZK -nonInteractive || true
docker compose exec -T namenode1 hdfs --daemon start zkfc || true
sleep 10

echo "=== [BOOTSTRAP 4/7] Bootstrapping namenode2 from namenode1 ==="
# NOTE: the one-off container's hostname is random, so Hadoop cannot infer
# our NN id from the rpc-address list — inject it the bde2020-native way:
# configure.sh renders HDFS_CONF_* into hdfs-site.xml at startup, so this
# sets dfs.ha.namenode.id=nn2 inside the one-off container only.
# (HADOOP_OPTS/-D CLI flags do NOT reach BootstrapStandby.parseArgs.)
if docker compose exec -T namenode2 test -f /hadoop/dfs/name/current/VERSION 2>/dev/null; then
  echo "namenode2 already bootstrapped, skipping."
else
  # Exit 5 = "data already exists, not formatting" (idempotent re-run race):
  # accept it, fail on anything else.
  set +e
  docker compose run --rm -e HDFS_CONF_dfs_ha_namenode_id=nn2 \
    namenode2 hdfs namenode -bootstrapStandby -nonInteractive
  BS_STATUS=$?
  set -e
  if [ "$BS_STATUS" -eq 5 ]; then
    echo "bootstrapStandby: data already present (status 5), continuing."
  elif [ "$BS_STATUS" -ne 0 ]; then
    echo "bootstrapStandby failed with status $BS_STATUS" >&2
    exit "$BS_STATUS"
  fi
fi

echo "=== [BOOTSTRAP 5/7] Starting namenode2 + zkfc ==="
docker compose up -d namenode2
poll_ui http://localhost:9871/jmx 60
docker compose exec -T namenode2 hdfs --daemon start zkfc || true
sleep 10

echo "=== [BOOTSTRAP 6/7] Starting datanode1 + datanode2 ==="
docker compose up -d datanode1 datanode2
sleep 10

echo "=== [BOOTSTRAP 7/7] Starting spark client ==="
docker compose up -d --build spark
docker compose ps

echo "=== BOOTSTRAP COMPLETE ==="
echo "Active/standby election may take ~30-60s. Run ./verify_gate1.sh to check."
docker compose exec -T namenode1 hdfs haadmin -getServiceState nn1 || true
docker compose exec -T namenode1 hdfs haadmin -getServiceState nn2 || true
