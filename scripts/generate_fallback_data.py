#!/usr/bin/env python3
"""Offline fallback generator: 1000-row NYC TLC parquet + zone lookup CSV.

Runs EITHER on the host (pyarrow/pandas) OR inside the spark container
(pyspark, already bundled in bitnami/spark — no network needed).
Schema matches the official yellow_tripdata NYC TLC feed.
Deterministic (seed=42) so re-runs are identical.
"""
import csv
import os
import random
import sys
from datetime import datetime, timedelta

DATA_DIR = os.environ.get("DATA_DIR", "/workspace/data")
TRIP_FILE = os.environ.get("TRIP_FILE", os.path.join(DATA_DIR, "yellow_tripdata_2023-01.parquet"))
ZONE_FILE = os.environ.get("ZONE_FILE", os.path.join(DATA_DIR, "taxi_zone_lookup.csv"))
N_ROWS = int(os.environ.get("N_ROWS", "1000"))

ZONES = [
    (1, "Manhattan", "Midtown Center", "Yellow Zone"),
    (2, "Queens", "Jamaica Bay", "Boro Zone"),
    (3, "Bronx", "Mott Haven/Port Morris", "Boro Zone"),
    (4, "Manhattan", "Times Sq/Theatre District", "Yellow Zone"),
    (5, "Brooklyn", "Williamsburg (North Side)", "Boro Zone"),
    (6, "Manhattan", "Upper East Side South", "Yellow Zone"),
    (7, "Queens", "Astoria", "Boro Zone"),
    (8, "Brooklyn", "Park Slope", "Boro Zone"),
    (9, "Staten Island", "St. George/New Brighton", "Boro Zone"),
    (10, "Manhattan", "Financial District South", "Yellow Zone"),
    (11, "Queens", "JFK Airport", "Airports"),
    (12, "Queens", "LaGuardia Airport", "Airports"),
    (13, "Brooklyn", "Brooklyn Heights", "Boro Zone"),
    (14, "Bronx", "Yankee Stadium", "Boro Zone"),
    (15, "Manhattan", "Upper West Side South", "Yellow Zone"),
]

COLUMNS = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
           "passenger_count", "trip_distance", "RatecodeID",
           "store_and_fwd_flag", "PULocationID", "DOLocationID",
           "payment_type", "fare_amount", "extra", "mta_tax",
           "tip_amount", "tolls_amount", "improvement_surcharge",
           "total_amount", "congestion_surcharge", "airport_fee"]


def write_zones(path):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["LocationID", "Borough", "Zone", "service_zone"])
        w.writerows(ZONES)
    print(f"wrote {path} ({len(ZONES)} zones)")


def gen_rows(n):
    rng = random.Random(42)
    base = datetime(2023, 1, 1, 0, 0, 0)
    zone_ids = [z[0] for z in ZONES]
    rows = []
    for _ in range(n):
        pickup = base + timedelta(minutes=rng.randint(0, 31 * 24 * 60 - 60))
        duration = rng.randint(2, 90)
        dropoff = pickup + timedelta(minutes=duration)
        passenger_count = rng.choice([1, 1, 1, 2, 2, 3, 3, 4, 5, 6])
        trip_distance = round(rng.uniform(0.3, 25.0), 2)
        pu, do = rng.choice(zone_ids), rng.choice(zone_ids)
        fare = round(3.0 + trip_distance * rng.uniform(2.2, 3.1), 2)
        tip = round(fare * rng.choice([0, 0, 0.1, 0.15, 0.2, 0.25]), 2)
        total = round(fare + 0.5 + 0.5 + tip + 1.0 + rng.choice([0.0, 2.5]), 2)
        rows.append({
            "VendorID": rng.choice([1, 2]),
            "tpep_pickup_datetime": pickup.strftime("%Y-%m-%d %H:%M:%S"),
            "tpep_dropoff_datetime": dropoff.strftime("%Y-%m-%d %H:%M:%S"),
            "passenger_count": passenger_count,
            "trip_distance": float(trip_distance),
            "RatecodeID": 1,
            "store_and_fwd_flag": rng.choice(["Y", "N"]),
            "PULocationID": pu,
            "DOLocationID": do,
            "payment_type": rng.choice([1, 1, 1, 2]),
            "fare_amount": float(fare),
            "extra": 0.5,
            "mta_tax": 0.5,
            "tip_amount": float(tip),
            "tolls_amount": 0.0,
            "improvement_surcharge": 1.0,
            "total_amount": float(total),
            "congestion_surcharge": 2.5,
            "airport_fee": 0.0,
        })
    return rows


def write_parquet_pyspark(rows, path):
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName("fallback-gen").master("local[*]")
             .config("spark.ui.enabled", "false").getOrCreate())
    try:
        df = spark.createDataFrame(rows)
        df = df.select(*COLUMNS)
        df.coalesce(1).write.mode("overwrite").parquet(path)
        print(f"wrote {path} ({len(rows)} rows) via pyspark")
    finally:
        spark.stop()


def write_parquet_pyarrow(rows, path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    table = pa.Table.from_pylist(rows, schema=pa.schema([
        ("VendorID", pa.int64()), ("tpep_pickup_datetime", pa.string()),
        ("tpep_dropoff_datetime", pa.string()), ("passenger_count", pa.int64()),
        ("trip_distance", pa.float64()), ("RatecodeID", pa.int64()),
        ("store_and_fwd_flag", pa.string()), ("PULocationID", pa.int64()),
        ("DOLocationID", pa.int64()), ("payment_type", pa.int64()),
        ("fare_amount", pa.float64()), ("extra", pa.float64()),
        ("mta_tax", pa.float64()), ("tip_amount", pa.float64()),
        ("tolls_amount", pa.float64()), ("improvement_surcharge", pa.float64()),
        ("total_amount", pa.float64()), ("congestion_surcharge", pa.float64()),
        ("airport_fee", pa.float64())]))
    pq.write_table(table, path)
    print(f"wrote {path} ({len(rows)} rows) via pyarrow")


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(ZONE_FILE):
        write_zones(ZONE_FILE)
    else:
        print(f"zone file exists, keeping: {ZONE_FILE}")
    if os.path.exists(TRIP_FILE):
        print(f"trip file exists, keeping: {TRIP_FILE}")
        return 0
    rows = gen_rows(N_ROWS)
    # Prefer pyspark when running inside the spark container (offline-safe),
    # fall back to pyarrow on the host.
    if os.environ.get("PREFER_PYSPARK") == "1":
        write_parquet_pyspark(rows, TRIP_FILE)
        return 0
    try:
        write_parquet_pyspark(rows, TRIP_FILE)
    except Exception as ex:  # noqa: BLE001 — pyspark missing on host
        print(f"pyspark unavailable ({ex}), trying pyarrow…")
        write_parquet_pyarrow(rows, TRIP_FILE)
    return 0


if __name__ == "__main__":
    sys.exit(main())
