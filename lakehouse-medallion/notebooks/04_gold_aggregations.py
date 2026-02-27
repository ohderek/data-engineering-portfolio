# Databricks notebook source
# 04_gold_aggregations.py
# ─────────────────────────────────────────────────────────────────────────────
# Gold layer — pre-aggregated analytics marts.
#
# Three tables:
#   gold.trips_by_zone    — daily trip counts, fares, and averages by pickup zone
#   gold.hourly_patterns  — demand and fare patterns by hour-of-day and day-of-week
#   gold.fare_analysis    — fare and tip breakdown by payment type and trip category
#
# All gold tables:
#   - Filter to dq_valid = true (exclude dirty rows from bronze/silver)
#   - Join silver.trips to bronze.raw_zones for zone names
#   - Use OVERWRITE mode — gold is always fully rebuilt from clean silver
#   - Use percentile_approx for median/P95 (cheaper than exact percentile on 10M+ rows)
#
# Time travel:
#   - Each OVERWRITE creates a new Delta version, enabling historical snapshots
#   - e.g. compare today's zone rankings vs. last week's via TIMESTAMP AS OF
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------

SILVER_TABLE = "taxi_lakehouse.silver.trips"
ZONES_TABLE  = "taxi_lakehouse.bronze.raw_zones"

from pyspark.sql import functions as F

# ── Base clean dataset — DQ-valid rows only ───────────────────────────────────

trips = (
    spark.table(SILVER_TABLE)
         .filter("dq_valid = true")
)

zones = (
    spark.table(ZONES_TABLE)
         .select(
             F.col("LocationID").alias("location_id"),
             F.col("Zone").alias("zone_name"),
             F.col("Borough").alias("borough"),
         )
)

print(f"Clean rows feeding gold: {trips.count():,}")

# COMMAND ----------

# ── Gold 1: Trips by pickup zone (daily grain) ────────────────────────────────
# Grain: pickup_date × pu_location_id
# Join to zones for human-readable zone / borough names.

trips_by_zone = (
    trips
    .join(zones, trips.pu_location_id == zones.location_id, "left")
    .groupBy(
        F.to_date("pickup_datetime").alias("pickup_date"),
        "pu_location_id",
        F.col("zone_name").alias("pu_zone"),
        F.col("borough").alias("pu_borough"),
    )
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare_amount"),          2).alias("avg_fare"),
        F.round(F.avg("tip_pct"),              4).alias("avg_tip_pct"),
        F.round(F.avg("trip_distance_miles"),  2).alias("avg_trip_distance"),
        F.round(F.avg("trip_duration_minutes"),2).alias("avg_duration_minutes"),
        F.round(F.sum("total_amount"),         2).alias("total_revenue"),
    )
    .orderBy("pickup_date", F.col("trip_count").desc())
)

(
    trips_by_zone
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("taxi_lakehouse.gold.trips_by_zone")
)

print(f"gold.trips_by_zone written — {trips_by_zone.count():,} rows")

# COMMAND ----------

# ── Gold 2: Hourly demand patterns ────────────────────────────────────────────
# Grain: hour_of_day × day_of_week
# Aggregated across the full dataset (not per-date) — useful for demand forecasting.

DOW_MAP = F.create_map(
    F.lit(1), F.lit("Sunday"),   F.lit(2), F.lit("Monday"),
    F.lit(3), F.lit("Tuesday"),  F.lit(4), F.lit("Wednesday"),
    F.lit(5), F.lit("Thursday"), F.lit(6), F.lit("Friday"),
    F.lit(7), F.lit("Saturday"),
)

hourly_patterns = (
    trips
    .withColumn("hour_of_day",  F.hour("pickup_datetime"))
    .withColumn("day_of_week",  DOW_MAP[F.dayofweek("pickup_datetime")])
    .withColumn("is_weekend",   F.dayofweek("pickup_datetime").isin(1, 7))
    .groupBy("hour_of_day", "day_of_week", "is_weekend")
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare_amount"),           2).alias("avg_fare"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
        F.round(F.avg("trip_distance_miles"),   2).alias("avg_trip_distance"),
        F.round(
            F.sum(F.col("is_airport_trip").cast("int")) / F.count("*"), 4
        ).alias("pct_airport_trips"),
    )
    .orderBy("day_of_week", "hour_of_day")
)

(
    hourly_patterns
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("taxi_lakehouse.gold.hourly_patterns")
)

print(f"gold.hourly_patterns written — {hourly_patterns.count():,} rows")

# COMMAND ----------

# ── Gold 3: Fare analysis by payment type ─────────────────────────────────────
# Grain: payment_type × rate_code × is_airport_trip
# Includes median and P95 fare using percentile_approx (efficient on large datasets).

fare_analysis_raw = (
    trips
    .groupBy("payment_type", "rate_code", "is_airport_trip")
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare_amount"),                                 2).alias("avg_fare"),
        F.round(F.avg("tip_pct"),                                     4).alias("avg_tip_pct"),
        F.round(F.percentile_approx("fare_amount", 0.5),              2).alias("median_fare"),
        F.round(F.percentile_approx("fare_amount", 0.95),             2).alias("p95_fare"),
    )
)

total_trips = trips.count()

fare_analysis = (
    fare_analysis_raw
    .withColumn("pct_of_total_trips",
        F.round(F.col("trip_count") / F.lit(total_trips), 4))
    .orderBy(F.col("trip_count").desc())
)

(
    fare_analysis
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("taxi_lakehouse.gold.fare_analysis")
)

print(f"gold.fare_analysis written — {fare_analysis.count():,} rows")

# COMMAND ----------

# ── Sample analytics queries ───────────────────────────────────────────────────

print("=" * 60)
print("TOP 10 PICKUP ZONES BY TOTAL TRIPS")
print("=" * 60)

(
    spark.table("taxi_lakehouse.gold.trips_by_zone")
    .groupBy("pu_zone", "pu_borough")
    .agg(F.sum("trip_count").alias("total_trips"),
         F.round(F.avg("avg_fare"), 2).alias("avg_fare"))
    .orderBy(F.col("total_trips").desc())
    .limit(10)
    .show(truncate=False)
)

print("=" * 60)
print("BUSIEST HOURS (WEEKDAY vs WEEKEND)")
print("=" * 60)

(
    spark.table("taxi_lakehouse.gold.hourly_patterns")
    .select("hour_of_day", "is_weekend", "trip_count", "avg_fare")
    .orderBy("is_weekend", F.col("trip_count").desc())
    .show(20)
)

print("=" * 60)
print("FARE BY PAYMENT TYPE")
print("=" * 60)

(
    spark.table("taxi_lakehouse.gold.fare_analysis")
    .select("payment_type", "trip_count", "avg_fare", "avg_tip_pct", "median_fare", "pct_of_total_trips")
    .orderBy(F.col("trip_count").desc())
    .show()
)

# COMMAND ----------

# ── Delta time travel demo ────────────────────────────────────────────────────
# Show how to query a previous version of a gold table.
# Useful for: comparing this week's zone rankings to last week's.

print("Delta table history (gold.trips_by_zone):")
spark.sql("DESCRIBE HISTORY taxi_lakehouse.gold.trips_by_zone").select(
    "version", "timestamp", "operation", "operationParameters"
).show(5, truncate=False)

# To query a prior version:
# spark.read.format("delta") \
#     .option("versionAsOf", 0) \
#     .table("taxi_lakehouse.gold.trips_by_zone") \
#     .show(5)

# COMMAND ----------

# ── VACUUM gold tables (optional — run weekly) ────────────────────────────────
# Removes files older than the retention threshold (default 7 days).
# Keeps Delta time travel available for up to the retention window.

# for table in ["trips_by_zone", "hourly_patterns", "fare_analysis"]:
#     spark.sql(f"VACUUM taxi_lakehouse.gold.{table} RETAIN 168 HOURS")
#     print(f"VACUUM complete: gold.{table}")
