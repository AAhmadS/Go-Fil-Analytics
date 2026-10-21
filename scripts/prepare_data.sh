#!/usr/bin/env bash
# Gate 2 — step 1: ensure ./data holds both inputs, downloading when online
# and generating a valid 1000-row offline fallback when downloads fail.
set -euo pipefail
export MSYS_NO_PATHCONV=1
# Git Bash (MSYS2) rewrites leading-slash args to C:\... for native docker.exe
# — disabled so container paths and hdfs:// URIs pass through untouched.
cd "$(dirname "$0")/.."

DATA_DIR="./data"
ZONE_FILE="$DATA_DIR/taxi_zone_lookup.csv"
FALLBACK_TRIP="$DATA_DIR/yellow_tripdata_2023-01.parquet"
ZONE_URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
TRIP_URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"

mkdir -p "$DATA_DIR"

have_trip=false
if ls "$DATA_DIR"/yellow_tripdata_*.parquet >/dev/null 2>&1; then have_trip=true; fi

echo "=== [DATA 1/3] taxi_zone_lookup.csv ==="
if [ -s "$ZONE_FILE" ]; then
  echo "exists locally, keeping: $ZONE_FILE"
else
  echo "downloading $ZONE_URL ..."
  if curl -fSL --connect-timeout 15 --max-time 120 -o "$ZONE_FILE" "$ZONE_URL"; then
    echo "downloaded zone lookup."
  else
    echo "download failed/blocked -> offline fallback generator."
    rm -f "$ZONE_FILE"
    DATA_DIR="$DATA_DIR" ZONE_FILE="$ZONE_FILE" TRIP_FILE="$FALLBACK_TRIP" python3 scripts/generate_fallback_data.py
  fi
fi

echo "=== [DATA 2/3] yellow_tripdata_*.parquet ==="
if [ "$have_trip" = true ]; then
  echo "exists locally, keeping:"
  ls -lh "$DATA_DIR"/yellow_tripdata_*.parquet
else
  echo "downloading sample $TRIP_URL ..."
  if curl -fSL --connect-timeout 15 --max-time 300 -o "$FALLBACK_TRIP" "$TRIP_URL"; then
    echo "downloaded trip sample."
  else
    echo "download failed/blocked -> offline fallback generator (1000 rows, Spark-compatible)."
    rm -f "$FALLBACK_TRIP"
    if docker compose ps --status running spark >/dev/null 2>&1 && docker compose ps spark 2>/dev/null | grep -q "Up\|running"; then
      echo "generating inside spark container (pyspark, fully offline)..."
      PREFER_PYSPARK=1 docker compose exec -T spark python3 /workspace/scripts/generate_fallback_data.py \
        || python3 scripts/generate_fallback_data.py
    else
      python3 scripts/generate_fallback_data.py
    fi
  fi
fi

echo "=== [DATA 3/3] local inventory ==="
ls -lh "$DATA_DIR"
echo "--- zone header ---"
head -n 5 "$ZONE_FILE"
echo "--- trip files ---"
ls -lh "$DATA_DIR"/yellow_tripdata_*.parquet
