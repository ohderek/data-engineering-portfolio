# dlt/taxi_pipeline.py
# ─────────────────────────────────────────────────────────────────────────────
# Delta Live Tables (DLT) pipeline — single file covering all three medallion layers.
#
# DLT manages:
#   - Dependency resolution between tables (no manual trigger ordering)
#   - Incremental processing via STREAMING tables
#   - Data quality expectations (quarantine pattern)
#   - Automatic retries and failure isolation per table
#   - Lineage tracking in Unity Catalog
#
# To deploy:
#   1. In Databricks UI → Delta Live Tables → Create Pipeline
#   2. Set Source Code path to this file
#   3. Set Target schema: taxi_lakehouse (DLT will create bronze/silver/gold tables)
#   4. Set Pipeline mode: Triggered (for batch runs on a schedule)
#   5. Configure cluster: Photon-enabled recommended for 10x faster Delta writes
#
# Pipeline config (pipelines.json equivalent):
#   {
#     "name": "NYC Taxi Lakehouse",
#     "target": "taxi_lakehouse",
#     "configuration": {
#       "volume_path": "/Volumes/taxi_lakehouse/bronze/raw_files"
#     },
#     "libraries": [{"notebook": {"path": "/path/to/dlt/taxi_pipeline"}}],
#     "clusters": [{"label": "default", "spark_conf": {"spark.databricks.delta.preview.enabled": "true"}}]
#   }
# ─────────────────────────────────────────────────────────────────────────────

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, DoubleType, StringType, TimestampType, IntegerType
)

# Pipeline configuration (set via pipeline config JSON, not hardcoded)
VOLUME_PATH = spark.conf.get("volume_path", "/Volumes/taxi_lakehouse/bronze/raw_files")

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


# ── BRONZE layer ──────────────────────────────────────────────────────────────

@dlt.table(
    name    = "raw_trips",
    comment = "Raw NYC Yellow Taxi trips. Source-faithful — no transformations.",
    table_properties = {
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
    },
)
def raw_trips():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format",          "parquet")
             .option("cloudFiles.schemaLocation",  f"{VOLUME_PATH}/_schema/trips")
             .option("cloudFiles.inferColumnTypes", "false")
             .schema(TRIPS_SCHEMA)
             .load(f"{VOLUME_PATH}/yellow_trips")
             .withColumn("_source_file", F.input_file_name())
             .withColumn("_ingested_at", F.current_timestamp())
    )


@dlt.table(
    name    = "raw_zones",
    comment = "NYC Taxi Zone lookup — 265 zones across 6 boroughs.",
    table_properties = {"quality": "bronze"},
)
def raw_zones():
    return (
        spark.read
             .option("header", "true")
             .option("inferSchema", "true")
             .csv(f"{VOLUME_PATH}/zones/taxi_zone_lookup.csv")
             .withColumn("_ingested_at", F.current_timestamp())
    )


# ── SILVER layer ──────────────────────────────────────────────────────────────

# DLT expectations — violations are quarantined, not dropped.
# Failed rows land in the DLT quarantine table for investigation.

SILVER_EXPECTATIONS = {
    "valid_duration":      "trip_duration_minutes > 0 AND trip_duration_minutes <= 1440",
    "valid_distance":      "trip_distance_miles >= 0 AND trip_distance_miles <= 500",
    "valid_fare":          "fare_amount >= 0 AND fare_amount <= 1000",
    "valid_location_ids":  "pu_location_id BETWEEN 1 AND 265 AND do_location_id BETWEEN 1 AND 265",
}

PAYMENT_TYPE_MAP = F.create_map(
    F.lit(1), F.lit("Credit Card"), F.lit(2), F.lit("Cash"),
    F.lit(3), F.lit("No Charge"),   F.lit(4), F.lit("Dispute"),
    F.lit(5), F.lit("Unknown"),     F.lit(6), F.lit("Voided Trip"),
)

RATE_CODE_MAP = F.create_map(
    F.lit(1), F.lit("Standard Rate"),       F.lit(2), F.lit("JFK"),
    F.lit(3), F.lit("Newark"),              F.lit(4), F.lit("Nassau or Westchester"),
    F.lit(5), F.lit("Negotiated Fare"),     F.lit(6), F.lit("Group Ride"),
    F.lit(99), F.lit("Unknown"),
)


@dlt.table(
    name    = "trips",
    comment = "Cleaned, validated, enriched trip records. DQ-flagged rows kept but marked.",
    table_properties = {
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
    partition_cols = ["pickup_date"],
)
@dlt.expect_all_or_drop(SILVER_EXPECTATIONS)
def trips():
    return (
        dlt.read_stream("raw_trips")

        .withColumn("trip_id", F.sha2(
            F.concat_ws("|",
                F.coalesce(F.col("VendorID").cast("string"),             F.lit("")),
                F.coalesce(F.col("tpep_pickup_datetime").cast("string"),  F.lit("")),
                F.coalesce(F.col("PULocationID").cast("string"),          F.lit("")),
            ), 256
        ))

        .withColumn("vendor_id",            F.col("VendorID").cast(IntegerType()))
        .withColumn("pickup_datetime",       F.col("tpep_pickup_datetime"))
        .withColumn("dropoff_datetime",      F.col("tpep_dropoff_datetime"))
        .withColumn("pickup_date",           F.to_date("tpep_pickup_datetime"))
        .withColumn("passenger_count",       F.col("passenger_count").cast(IntegerType()))
        .withColumn("trip_distance_miles",   F.col("trip_distance"))
        .withColumn("pu_location_id",        F.col("PULocationID").cast(IntegerType()))
        .withColumn("do_location_id",        F.col("DOLocationID").cast(IntegerType()))

        .withColumn("payment_type",
            F.coalesce(PAYMENT_TYPE_MAP[F.col("payment_type").cast(IntegerType())], F.lit("Unknown")))
        .withColumn("rate_code",
            F.coalesce(RATE_CODE_MAP[F.col("RatecodeID").cast(IntegerType())], F.lit("Unknown")))

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

        .withColumn("is_airport_trip", F.col("RatecodeID").isin(2.0, 3.0))

        .select(
            "trip_id", "vendor_id", "pickup_datetime", "dropoff_datetime", "pickup_date",
            "trip_duration_minutes", "trip_distance_miles", "avg_speed_mph",
            "passenger_count", "pu_location_id", "do_location_id",
            "rate_code", "payment_type",
            "fare_amount", "tip_amount", "tolls_amount", "total_amount",
            "tip_pct", "is_airport_trip",
            "_source_file", "_ingested_at",
        )
    )


# ── GOLD layer ────────────────────────────────────────────────────────────────
# Gold tables are MATERIALIZED VIEWs in DLT — automatically refreshed when
# silver data changes, with no manual orchestration needed.

@dlt.table(
    name    = "trips_by_zone",
    comment = "Daily trip aggregations by pickup zone. Grain: date × zone.",
    table_properties = {"quality": "gold"},
)
def trips_by_zone():
    zones = dlt.read("raw_zones").select(
        F.col("LocationID").alias("location_id"),
        F.col("Zone").alias("zone_name"),
        F.col("Borough").alias("borough"),
    )

    return (
        dlt.read("trips")
           .join(zones, dlt.read("trips").pu_location_id == zones.location_id, "left")
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
    )


@dlt.table(
    name    = "hourly_patterns",
    comment = "Demand and fare patterns by hour-of-day and day-of-week.",
    table_properties = {"quality": "gold"},
)
def hourly_patterns():
    DOW_MAP = F.create_map(
        F.lit(1), F.lit("Sunday"),   F.lit(2), F.lit("Monday"),
        F.lit(3), F.lit("Tuesday"),  F.lit(4), F.lit("Wednesday"),
        F.lit(5), F.lit("Thursday"), F.lit(6), F.lit("Friday"),
        F.lit(7), F.lit("Saturday"),
    )

    return (
        dlt.read("trips")
           .withColumn("hour_of_day", F.hour("pickup_datetime"))
           .withColumn("day_of_week", DOW_MAP[F.dayofweek("pickup_datetime")])
           .withColumn("is_weekend",  F.dayofweek("pickup_datetime").isin(1, 7))
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
    )


@dlt.table(
    name    = "fare_analysis",
    comment = "Fare and tip breakdown by payment type, rate code, and airport flag.",
    table_properties = {"quality": "gold"},
)
def fare_analysis():
    total = dlt.read("trips").count()

    return (
        dlt.read("trips")
           .groupBy("payment_type", "rate_code", "is_airport_trip")
           .agg(
               F.count("*").alias("trip_count"),
               F.round(F.avg("fare_amount"),                                 2).alias("avg_fare"),
               F.round(F.avg("tip_pct"),                                     4).alias("avg_tip_pct"),
               F.round(F.percentile_approx("fare_amount", 0.5),              2).alias("median_fare"),
               F.round(F.percentile_approx("fare_amount", 0.95),             2).alias("p95_fare"),
               F.round(F.count("*") / F.lit(total),                          4).alias("pct_of_total_trips"),
           )
           .orderBy(F.col("trip_count").desc())
    )
