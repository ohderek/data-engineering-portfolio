# Databricks notebook source
# 03_silver_transform.py
# ─────────────────────────────────────────────────────────────────────────────
# Silver layer — clean, validate, enrich, and deduplicate bronze trips.
#
# Transformations applied:
#   - Cast raw types to correct analytic types
#   - Decode integer enums → human-readable labels (payment type, rate code)
#   - Compute derived fields: duration, speed, tip %, airport flag
#   - Generate surrogate key (SHA2 hash of natural key)
#   - Apply data quality rules and flag invalid records
#   - Deduplicate within each source file using the surrogate key
#   - MERGE INTO silver.trips (upsert by trip_id)
#   - Z-ORDER after load for fast downstream queries
#
# Processing mode:
#   - Reads bronze Change Data Feed (only rows new since last run)
#   - Falls back to full table scan on first run
#   - Idempotent: replaying the same bronze data produces the same silver output
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------

dbutils.widgets.text("batch_size", "0", "Max bronze rows to process (0 = all)")

MAX_ROWS = int(dbutils.widgets.get("batch_size"))  # 0 = no limit

BRONZE_CATALOG = "taxi_lakehouse"
SILVER_CATALOG = "taxi_lakehouse"
BRONZE_TABLE   = f"{BRONZE_CATALOG}.bronze.raw_trips"
SILVER_TABLE   = f"{SILVER_CATALOG}.silver.trips"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from delta.tables import DeltaTable

# ── Read bronze — use Change Data Feed for incremental runs ──────────────────
# On the very first run, CDF won't have a starting version so we read the full table.

try:
    last_version = spark.sql(
        f"SELECT MAX(silver_load_version) FROM {SILVER_TABLE}"
    ).collect()[0][0]

    if last_version is None:
        raise ValueError("No prior silver version — falling back to full scan")

    bronze_df = (
        spark.read
             .format("delta")
             .option("readChangeFeed", "true")
             .option("startingVersion", last_version + 1)
             .table(BRONZE_TABLE)
             .filter(F.col("_change_type").isin("insert", "update_postimage"))
    )
    print(f"Incremental read from bronze version {last_version + 1}")

except Exception:
    bronze_df = spark.table(BRONZE_TABLE)
    print("Full table scan (first run or CDF unavailable)")

if MAX_ROWS > 0:
    bronze_df = bronze_df.limit(MAX_ROWS)

# COMMAND ----------

# ── Payment type and rate code lookups ────────────────────────────────────────
# TLC data dictionary: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

PAYMENT_TYPE_MAP = {
    1: "Credit Card",
    2: "Cash",
    3: "No Charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided Trip",
}

RATE_CODE_MAP = {
    1: "Standard Rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated Fare",
    6: "Group Ride",
    99: "Unknown",
}

payment_map_expr = F.create_map(*[
    item for kv in PAYMENT_TYPE_MAP.items()
    for item in (F.lit(kv[0]), F.lit(kv[1]))
])

rate_map_expr = F.create_map(*[
    item for kv in RATE_CODE_MAP.items()
    for item in (F.lit(kv[0]), F.lit(kv[1]))
])

# COMMAND ----------

# ── Build silver dataframe ────────────────────────────────────────────────────

silver_df = (
    bronze_df

    # ── Surrogate key ─────────────────────────────────────────────────────────
    # Natural key: vendor + pickup timestamp + pickup location.
    # SHA2 with 256 bits — deterministic dedup key.
    .withColumn("trip_id", F.sha2(
        F.concat_ws("|",
            F.coalesce(F.col("VendorID").cast("string"),          F.lit("")),
            F.coalesce(F.col("tpep_pickup_datetime").cast("string"), F.lit("")),
            F.coalesce(F.col("PULocationID").cast("string"),       F.lit("")),
        ), 256
    ))

    # ── Cast and rename ────────────────────────────────────────────────────────
    .withColumn("vendor_id",          F.col("VendorID").cast(IntegerType()))
    .withColumn("pickup_datetime",    F.col("tpep_pickup_datetime"))
    .withColumn("dropoff_datetime",   F.col("tpep_dropoff_datetime"))
    .withColumn("passenger_count",    F.col("passenger_count").cast(IntegerType()))
    .withColumn("trip_distance_miles", F.col("trip_distance"))
    .withColumn("pu_location_id",     F.col("PULocationID").cast(IntegerType()))
    .withColumn("do_location_id",     F.col("DOLocationID").cast(IntegerType()))

    # ── Decode enums ──────────────────────────────────────────────────────────
    .withColumn("payment_type",
        F.coalesce(payment_map_expr[F.col("payment_type").cast(IntegerType())],
                   F.lit("Unknown")))
    .withColumn("rate_code",
        F.coalesce(rate_map_expr[F.col("RatecodeID").cast(IntegerType())],
                   F.lit("Unknown")))

    # ── Derived metrics ───────────────────────────────────────────────────────
    .withColumn("trip_duration_minutes",
        (F.unix_timestamp("tpep_dropoff_datetime") -
         F.unix_timestamp("tpep_pickup_datetime")) / 60.0)

    .withColumn("avg_speed_mph",
        F.when(F.col("trip_duration_minutes") > 0,
               F.col("trip_distance") / (F.col("trip_duration_minutes") / 60.0)
        ).otherwise(F.lit(None).cast(DoubleType())))

    .withColumn("tip_pct",
        F.when(F.col("fare_amount") > 0,
               F.col("tip_amount") / F.col("fare_amount")
        ).otherwise(F.lit(None).cast(DoubleType())))

    .withColumn("is_airport_trip",
        F.col("RatecodeID").isin(2.0, 3.0))

    # ── fare columns ─────────────────────────────────────────────────────────
    .withColumn("fare_amount",   F.col("fare_amount"))
    .withColumn("tip_amount",    F.col("tip_amount"))
    .withColumn("tolls_amount",  F.col("tolls_amount"))
    .withColumn("total_amount",  F.col("total_amount"))
)

# COMMAND ----------

# ── Data quality rules ─────────────────────────────────────────────────────────
# Soft failures: rows are kept but flagged. Hard failures excluded from gold.

DQ_RULES = {
    "invalid_duration":     "trip_duration_minutes <= 0 OR trip_duration_minutes > 1440",
    "invalid_distance":     "trip_distance_miles < 0 OR trip_distance_miles > 500",
    "invalid_fare":         "fare_amount < 0 OR fare_amount > 1000",
    "invalid_tip":          "tip_amount < 0",
    "invalid_total":        "total_amount < 0",
    "future_pickup":        "pickup_datetime > current_timestamp()",
    "pre_2009_data":        "YEAR(pickup_datetime) < 2009",
    "unknown_pu_location":  "pu_location_id IS NULL OR pu_location_id < 1 OR pu_location_id > 265",
    "zero_speed_long_trip": "avg_speed_mph = 0 AND trip_duration_minutes > 5",
    "implausible_speed":    "avg_speed_mph > 120",
}

dq_flags_expr = F.array(*[
    F.when(F.expr(rule_sql), F.lit(rule_name)).otherwise(F.lit(None))
    for rule_name, rule_sql in DQ_RULES.items()
])

silver_df = (
    silver_df
    .withColumn("_dq_flags_raw", dq_flags_expr)
    .withColumn("dq_flags",
        F.array_compact(F.col("_dq_flags_raw")))       # remove nulls
    .withColumn("dq_valid",
        F.size(F.col("dq_flags")) == 0)
    .drop("_dq_flags_raw")
)

# COMMAND ----------

# ── Select final silver schema ─────────────────────────────────────────────────

silver_final = silver_df.select(
    "trip_id",
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "trip_duration_minutes",
    "trip_distance_miles",
    "avg_speed_mph",
    "passenger_count",
    "pu_location_id",
    "do_location_id",
    "rate_code",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "tolls_amount",
    "total_amount",
    "tip_pct",
    "is_airport_trip",
    "dq_valid",
    "dq_flags",
    "_source_file",
    "_ingested_at",
)

# Deduplicate within the incoming batch (same trip_id from two source files)
silver_final = silver_final.dropDuplicates(["trip_id"])

print(f"Rows to merge: {silver_final.count():,}")
print(f"DQ valid rows: {silver_final.filter('dq_valid').count():,}")
print(f"DQ flagged rows: {silver_final.filter('NOT dq_valid').count():,}")

# COMMAND ----------

# ── MERGE INTO silver.trips ────────────────────────────────────────────────────
# Upsert pattern: insert new trips, update if the record has changed.
# This makes the pipeline safe to re-run (idempotent).

silver_final.createOrReplaceTempView("silver_incoming")

spark.sql(f"""
MERGE INTO {SILVER_TABLE} AS target
USING silver_incoming        AS source
ON target.trip_id = source.trip_id
WHEN MATCHED AND (
    target.dq_valid    != source.dq_valid OR
    target._source_file != source._source_file
) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print("Silver MERGE complete.")

# COMMAND ----------

# ── OPTIMIZE + Z-ORDER silver table ───────────────────────────────────────────
# Run after a significant load. Z-ORDER co-locates data by the columns most
# likely to appear in WHERE clauses and join predicates.

spark.sql(f"""
    OPTIMIZE {SILVER_TABLE}
    ZORDER BY (pu_location_id, pickup_datetime)
""")
print("Silver OPTIMIZE + Z-ORDER complete.")

# COMMAND ----------

# ── Post-load quality summary ─────────────────────────────────────────────────

silver = spark.table(SILVER_TABLE)

print(f"\nSilver table totals: {silver.count():,} rows")

print("\nDQ flag breakdown:")
(
    silver
    .filter("NOT dq_valid")
    .select(F.explode("dq_flags").alias("flag"))
    .groupBy("flag").count()
    .orderBy(F.col("count").desc())
    .show()
)

print("Payment type distribution:")
(
    silver
    .groupBy("payment_type").count()
    .orderBy(F.col("count").desc())
    .show()
)
