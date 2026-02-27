# Databricks notebook source
# 02_bronze_ingest.py
# ─────────────────────────────────────────────────────────────────────────────
# Bronze layer — raw ingestion using Databricks Auto Loader.
#
# Auto Loader (cloudFiles) streams new Parquet files from the Volume into a
# Delta table without reprocessing files already seen. This makes the pipeline
# idempotent and incrementally efficient as new monthly files arrive.
#
# Design decisions:
#   - No schema enforcement at bronze — source-faithful, never transform here
#   - _source_file and _ingested_at metadata columns added for lineage
#   - Trigger.AvailableNow processes all pending files then stops (batch mode)
#   - checkpointLocation persists Auto Loader's file-tracking state between runs
#   - enableChangeDataFeed on the Delta table feeds the silver merge downstream
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------

dbutils.widgets.text("volume_path",     "/Volumes/taxi_lakehouse/bronze/raw_files",
                     "Source Volume path")
dbutils.widgets.text("checkpoint_path", "/Volumes/taxi_lakehouse/bronze/_checkpoints/autoloader_trips",
                     "Auto Loader checkpoint path")

VOLUME_PATH      = dbutils.widgets.get("volume_path")
CHECKPOINT_PATH  = dbutils.widgets.get("checkpoint_path")
TRIPS_SOURCE     = f"{VOLUME_PATH}/yellow_trips"
ZONES_SOURCE     = f"{VOLUME_PATH}/zones"
BRONZE_CATALOG   = "taxi_lakehouse"
BRONZE_SCHEMA    = "bronze"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, DoubleType, StringType, TimestampType
)

# ── Explicit schema — prevents Auto Loader from sampling every file ────────────
# Mirrors the TLC data dictionary exactly.

TRIPS_SCHEMA = StructType([
    StructField("VendorID",              LongType(),      True),
    StructField("tpep_pickup_datetime",  TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count",       DoubleType(),    True),
    StructField("trip_distance",         DoubleType(),    True),
    StructField("RatecodeID",            DoubleType(),    True),
    StructField("store_and_fwd_flag",    StringType(),    True),
    StructField("PULocationID",          LongType(),      True),
    StructField("DOLocationID",          LongType(),      True),
    StructField("payment_type",          LongType(),      True),
    StructField("fare_amount",           DoubleType(),    True),
    StructField("extra",                 DoubleType(),    True),
    StructField("mta_tax",               DoubleType(),    True),
    StructField("tip_amount",            DoubleType(),    True),
    StructField("tolls_amount",          DoubleType(),    True),
    StructField("improvement_surcharge", DoubleType(),    True),
    StructField("total_amount",          DoubleType(),    True),
    StructField("congestion_surcharge",  DoubleType(),    True),
    StructField("airport_fee",           DoubleType(),    True),
])

# COMMAND ----------

# ── Auto Loader: stream Parquet files → bronze Delta table ────────────────────
# Trigger.AvailableNow = process all pending files, then stop.
# Equivalent to a batch run but with incremental tracking state.

(
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format",           "parquet")
         .option("cloudFiles.schemaLocation",   f"{CHECKPOINT_PATH}/schema")
         .option("cloudFiles.inferColumnTypes",  "false")
         .schema(TRIPS_SCHEMA)
         .load(TRIPS_SOURCE)

         # Add lineage columns before writing
         .withColumn("_source_file",  F.input_file_name())
         .withColumn("_ingested_at",  F.current_timestamp())

         .writeStream
         .format("delta")
         .option("checkpointLocation", f"{CHECKPOINT_PATH}/trips")
         .option("mergeSchema", "true")
         .trigger(availableNow=True)
         .toTable(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.raw_trips")
)

print("Auto Loader stream submitted — waiting for completion...")

# COMMAND ----------

# ── Zone lookup: one-time batch load ─────────────────────────────────────────
# Small CSV (<50 KB) — no need for Auto Loader, just overwrite each run.

(
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(ZONES_SOURCE)
         .withColumn("_ingested_at", F.current_timestamp())
         .write
         .format("delta")
         .mode("overwrite")
         .saveAsTable(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.raw_zones")
)

print("Zone lookup loaded.")

# COMMAND ----------

# ── Verify bronze layer ────────────────────────────────────────────────────────

bronze_trips = spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.raw_trips")
bronze_zones = spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.raw_zones")

print("Bronze layer row counts:")
print(f"  raw_trips : {bronze_trips.count():>12,}")
print(f"  raw_zones : {bronze_zones.count():>12,}")

# Rows by source file
print("\nRows per source file:")
(
    bronze_trips
    .groupBy("_source_file")
    .count()
    .orderBy("_source_file")
    .show(truncate=False)
)

# Date range sanity check
bronze_trips.selectExpr(
    "MIN(tpep_pickup_datetime) AS earliest_pickup",
    "MAX(tpep_pickup_datetime) AS latest_pickup",
).show()

# COMMAND ----------

# ── OPTIMIZE bronze table ──────────────────────────────────────────────────────
# Compacts small files written by Auto Loader micro-batches.
# Run after initial load; Auto Optimize handles ongoing compaction.

spark.sql(f"OPTIMIZE {BRONZE_CATALOG}.{BRONZE_SCHEMA}.raw_trips")
print("Bronze OPTIMIZE complete.")
