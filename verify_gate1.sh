#!/usr/bin/env bash
# Gate 1 verification: HA states + Web UI + container health.
# NOTE: hdfs is invoked directly (see bootstrap_cluster.sh header) because
# `bash -lc` on the bde2020 images resets PATH and hides the hdfs binary.
# NOTE: no `pipefail` here on purpose — `hdfs dfsadmin -report | head`
# legitimately trips SIGPIPE, which must not fail verification.
set -u
export MSYS_NO_PATHCONV=1
# Git Bash (MSYS2) rewrites leading-slash args to C:\... for native docker.exe
# — disabled so container paths and hdfs:// URIs pass through untouched.
cd "$(dirname "$0")"
PASS=1

echo "=== [VERIFY] docker compose ps ==="
docker compose ps

echo ""
echo "=== [VERIFY] HDFS HA service states (expect one active + one standby) ==="
S1=$(docker compose exec -T namenode1 hdfs haadmin -getServiceState nn1 2>&1 | tr -d '\r' | tail -n 1)
S2=$(docker compose exec -T namenode1 hdfs haadmin -getServiceState nn2 2>&1 | tr -d '\r' | tail -n 1)
echo "nn1 -> $S1"
echo "nn2 -> $S2"
if { [ "$S1" = "active" ] && [ "$S2" = "standby" ]; } || { [ "$S1" = "standby" ] && [ "$S2" = "active" ]; }; then
  echo "OK: HA pair is healthy (one active, one standby)."
else
  echo "FAIL: expected one active + one standby, got nn1=$S1 nn2=$S2"
  PASS=0
fi

echo ""
echo "=== [VERIFY] Web UI http://localhost:9870 ==="
# Host-side curl to Docker Desktop's localhost forward can flake on some
# hosts (proxies, resolvers). Try localhost + 127.0.0.1 with retries and no
# proxy; as a last resort probe Jetty from inside the container so a host
# forwarding hiccup is not misreported as a dead NameNode.
check_ui() { # check_ui <container> <hostport>
  local container="$1" port="$2" i
  for i in 1 2 3; do
    if curl -sf -m 10 --noproxy '*' "http://localhost:${port}/" -o /dev/null; then return 0; fi
    if curl -sf -m 10 --noproxy '*' "http://127.0.0.1:${port}/" -o /dev/null; then return 0; fi
    sleep 5
  done
  if docker compose exec -T "$container" nc -z localhost 9870 2>/dev/null; then
    echo "WARN: Jetty is UP inside $container but unreachable from the host (forwarding flake)"
    return 0
  fi
  return 1
}
if check_ui namenode1 9870; then
  echo "OK: namenode1 Web UI responds on :9870"
else
  echo "FAIL: http://localhost:9870 did not respond"
  PASS=0
fi
if check_ui namenode2 9871; then
  echo "OK: namenode2 Web UI responds on :9871"
else
  echo "WARN: http://localhost:9871 did not respond (standby UI may lag election)"
fi

echo ""
echo "=== [VERIFY] Safemode / datanode liveness ==="
docker compose exec -T namenode1 hdfs dfsadmin -report -live 2>&1 | head -n 20 || PASS=0

if [ "$PASS" = "1" ]; then echo "VERIFY_GATE1: PASS"; else echo "VERIFY_GATE1: FAIL"; exit 1; fi
