#!/usr/bin/env python3
"""
streaming_consumer.py
─────────────────────────────────────────────────────────────────────────────
PySpark Structured Streaming job that consumes order events from Kafka
and writes to a two-layer Delta Lake medallion.

Output layers
─────────────
  Bronze  — every parsed event, exactly as received, partitioned by date.
            Output mode: append. Trigger: 30-second micro-batches.

  Gold    — 5-minute tumbling-window aggregations per region × category.
            Metrics: revenue, order count, avg order value, unique customers.
            Output mode: update (only changed windows are written per batch).
            Watermark: 10 minutes — events more than 10 min late are dropped.

Key design decisions
────────────────────
  - Event time (event_timestamp in the payload) drives windowing, NOT the
    Kafka broker ingest timestamp. This gives correct results when the
    producer is slow or batches events.

  - Two independent streaming queries run concurrently. Each has its own
    checkpoint and Kafka offset cursor. Bronze and Gold never block each other.

  - The watermark sets the late-data tolerance. At 10 minutes:
      · Windows close (and are eligible for output) 10 minutes after
        event_time + window_duration.
      · Spark can safely discard state older than that, bounding memory use.
      · Tighten for lower memory, widen for more complete aggregations.

  - ORDER_CANCELLED and ORDER_UPDATED events land in Bronze (full audit trail)
    but are excluded from Gold revenue aggregations — only ORDER_PLACED counts.

Run:
    spark-submit \\
        --packages io.delta:delta-spark_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
        spark/streaming_consumer.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ── Paths ─────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC     = "order-events"

BRONZE_PATH     = "./delta/bronze/order_events"
GOLD_PATH       = "./delta/gold/order_revenue_windows"

CHECKPOINT_BASE = "./checkpoints"


# ── Schema ────────────────────────────────────────────────────────────────────
# Explicit schema is faster than inferring (no schema-inference scan) and
# enforces the producer → consumer contract. Mismatched fields land as NULL
# rather than causing job failure — the isNotNull filter below catches them.

ORDER_EVENT_SCHEMA = StructType([
    StructField("order_id",          StringType(),    nullable=False),
    StructField("customer_id",       StringType(),    nullable=True),
    StructField("product_id",        StringType(),    nullable=True),
    StructField("product_name",      StringType(),    nullable=True),
    StructField("product_category",  StringType(),    nullable=True),
    StructField("quantity",          IntegerType(),   nullable=True),
    StructField("unit_price",        DoubleType(),    nullable=True),
    StructField("total_price",       DoubleType(),    nullable=True),
    StructField("event_type",        StringType(),    nullable=True),
    StructField("region",            StringType(),    nullable=True),
    StructField("event_timestamp",   TimestampType(), nullable=False),
])


# ── Session ───────────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("OrderEventsStreaming")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


# ── Source ────────────────────────────────────────────────────────────────────

def read_kafka(spark: SparkSession):
    """Return a streaming DataFrame from the Kafka topic."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe",               KAFKA_TOPIC)
        # latest: skip historical messages on first start.
        # Checkpoints take over for subsequent restarts — this only applies
        # to the very first time the job runs against an empty checkpoint dir.
        .option("startingOffsets",         "latest")
        # Cap messages per micro-batch to avoid a slow-start avalanche when
        # the job is catching up after a restart.
        .option("maxOffsetsPerTrigger",    10_000)
        .load()
    )


# ── Parse ─────────────────────────────────────────────────────────────────────

def parse_events(raw_df):
    """
    Kafka delivers the message body as bytes in the `value` column.
    Cast to string, then parse JSON against the explicit schema.
    Rows where JSON parsing fails land with all schema fields as NULL;
    the isNotNull filter drops them rather than letting bad rows corrupt
    downstream aggregations.
    """
    return (
        raw_df
        .select(
            # Retain Kafka metadata for debugging / replay
            F.col("timestamp").alias("kafka_ingest_ts"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.from_json(
                F.col("value").cast("string"),
                ORDER_EVENT_SCHEMA,
            ).alias("data"),
        )
        .select(
            "kafka_ingest_ts",
            "kafka_partition",
            "kafka_offset",
            "data.*",
        )
        .filter(F.col("order_id").isNotNull())
    )


# ── Bronze sink ───────────────────────────────────────────────────────────────

def write_bronze(events_df) -> None:
    """
    Append every valid event to the Bronze Delta table.
    Partitioned by event_date for efficient downstream date-range scans.
    """
    (
        events_df
        .withColumn("event_date", F.to_date("event_timestamp"))
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze")
        .partitionBy("event_date")
        .trigger(processingTime="30 seconds")
        .start(BRONZE_PATH)
    )


# ── Gold sink ─────────────────────────────────────────────────────────────────

def write_gold(events_df) -> None:
    """
    Aggregate ORDER_PLACED events into 5-minute tumbling windows.

    Why update output mode?
    ───────────────────────
    In 'update' mode Spark only writes the windows that changed in the
    current micro-batch. For an aggregation table this is far more efficient
    than 'complete' mode (which rewrites every row every batch) and is the
    right choice when downstream consumers poll the latest window state.

    Why filter to ORDER_PLACED before watermarking?
    ───────────────────────────────────────────────
    Applying the event_type filter before groupBy means the watermark
    advances based solely on revenue-relevant events. This avoids a subtle
    correctness issue where a high-volume of ORDER_CANCELLED events (which
    arrive after placement) could push the watermark forward prematurely
    and close windows early.
    """
    windowed = (
        events_df
        .filter(F.col("event_type") == "ORDER_PLACED")
        # event_timestamp is the business time embedded in the payload.
        # 10-minute watermark: windows are finalised (and can be evicted from
        # state) once the watermark passes window_end + 10 minutes.
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            F.window("event_timestamp", "5 minutes").alias("window"),
            F.col("region"),
            F.col("product_category"),
        )
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("total_price").alias("total_revenue"),
            F.avg("total_price").alias("avg_order_value"),
            F.sum("quantity").alias("total_units_sold"),
            # countDistinct in streaming requires watermarking — safe here
            # because the watermark is already set above.
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "region",
            "product_category",
            "order_count",
            F.round("total_revenue",   2).alias("total_revenue"),
            F.round("avg_order_value", 2).alias("avg_order_value"),
            "total_units_sold",
            "unique_customers",
        )
    )

    (
        windowed
        .writeStream
        .format("delta")
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/gold")
        .trigger(processingTime="30 seconds")
        .start(GOLD_PATH)
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw_df    = read_kafka(spark)
    events_df = parse_events(raw_df)

    # Start both streaming queries. They run concurrently on separate threads,
    # each with their own Kafka offset cursor and checkpoint.
    write_bronze(events_df)
    write_gold(events_df)

    print(f"[streaming] Bronze  → {BRONZE_PATH}")
    print(f"[streaming] Gold    → {GOLD_PATH}")
    print(f"[streaming] Awaiting data. Ctrl+C to stop.\n")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
