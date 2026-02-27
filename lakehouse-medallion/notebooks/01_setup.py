# Databricks notebook source
# 01_setup.py
# ─────────────────────────────────────────────────────────────────────────────
# One-time environment setup. Run before any other notebook.
#
# What this does:
#   1. Downloads NYC Yellow Taxi Parquet files from the TLC public S3 bucket
#   2. Writes them to a Databricks Volume (Unity Catalog) or DBFS for ingestion
#   3. Downloads the Taxi Zone lookup CSV
#   4. Verifies file counts and sizes
#
# Dataset: NYC TLC Yellow Taxi Trip Records
#   URL:  https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
#   Files: Monthly Parquet, ~50–150 MB each, ~3M rows/month
#   License: NYC Open Data — free to use
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------

# Widget: choose how many months to download (1–12)
dbutils.widgets.dropdown("months_to_download", "3", [str(i) for i in range(1, 13)],
                         "Months to download (2024)")
dbutils.widgets.text("target_volume", "/Volumes/taxi_lakehouse/bronze/raw_files",
                     "Target Volume path")

MONTHS       = int(dbutils.widgets.get("months_to_download"))
VOLUME_PATH  = dbutils.widgets.get("target_volume")
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL     = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

print(f"Downloading {MONTHS} month(s) → {VOLUME_PATH}")

# COMMAND ----------

import requests
import os
from pathlib import Path

# ── Create target directories ─────────────────────────────────────────────────

trips_dir = f"{VOLUME_PATH}/yellow_trips"
zones_dir = f"{VOLUME_PATH}/zones"

dbutils.fs.mkdirs(trips_dir)
dbutils.fs.mkdirs(zones_dir)

# COMMAND ----------

# ── Download monthly trip Parquet files ───────────────────────────────────────
# Each file is named: yellow_tripdata_YYYY-MM.parquet
# We use 2024 data — January through the requested number of months.

downloaded = []

for month in range(1, MONTHS + 1):
    filename = f"yellow_tripdata_2024-{month:02d}.parquet"
    url      = f"{TLC_BASE_URL}/{filename}"
    local    = f"/tmp/{filename}"
    dest     = f"{trips_dir}/{filename}"

    print(f"  [{month}/{MONTHS}] Downloading {filename} ...", end=" ")

    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()

    with open(local, "wb") as f:
        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
            f.write(chunk)

    size_mb = os.path.getsize(local) / 1_048_576
    print(f"{size_mb:.1f} MB")

    # Copy from driver /tmp → Unity Catalog Volume
    dbutils.fs.cp(f"file://{local}", dest)
    os.remove(local)

    downloaded.append({"file": filename, "size_mb": round(size_mb, 1)})

print(f"\nDownloaded {len(downloaded)} file(s).")

# COMMAND ----------

# ── Download Taxi Zone lookup ──────────────────────────────────────────────────

zone_file = "/tmp/taxi_zone_lookup.csv"
response  = requests.get(ZONE_URL, timeout=30)
response.raise_for_status()

with open(zone_file, "wb") as f:
    f.write(response.content)

dbutils.fs.cp(f"file://{zone_file}", f"{zones_dir}/taxi_zone_lookup.csv")
os.remove(zone_file)

print("Zone lookup downloaded.")

# COMMAND ----------

# ── Verify files landed correctly ─────────────────────────────────────────────

from pyspark.sql.functions import col, count, sum as _sum

trip_files  = dbutils.fs.ls(trips_dir)
zone_files  = dbutils.fs.ls(zones_dir)

print(f"\nTrip files in Volume ({len(trip_files)}):")
for f in trip_files:
    print(f"  {f.name}  —  {f.size / 1_048_576:.1f} MB")

print(f"\nZone files:")
for f in zone_files:
    print(f"  {f.name}  —  {f.size / 1_048_576:.3f} MB")

# Quick row count check
df = spark.read.parquet(trips_dir)
print(f"\nTotal rows across all downloaded months: {df.count():,}")
print(f"Columns: {len(df.columns)}  —  {df.columns}")

# COMMAND ----------

# ── Print next steps ──────────────────────────────────────────────────────────

print("""
Setup complete.

Next steps:
  02_bronze_ingest.py  — ingest raw Parquet → Delta (bronze layer)
  03_silver_transform.py — clean, validate, enrich (silver layer)
  04_gold_aggregations.py — build analytics marts (gold layer)

Or run the full Delta Live Tables pipeline:
  dlt/taxi_pipeline.py — single DLT pipeline covering all three layers
""")
