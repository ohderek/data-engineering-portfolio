-- bronze_check.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Validate the Bronze Delta table: row counts, event type distribution,
-- null rates, and revenue sanity checks.
--
-- Load in PySpark before running:
--   spark.read.format("delta").load("./delta/bronze/order_events") \
--        .createOrReplaceTempView("bronze_order_events")
-- ─────────────────────────────────────────────────────────────────────────────


-- 1. Event type distribution per date
--    Expected: ~60% ORDER_PLACED, ~20% each for CANCELLED and UPDATED
SELECT
    event_date,
    event_type,
    COUNT(*)                                                          AS event_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY event_date), 1) AS pct_of_day
FROM bronze_order_events
GROUP BY event_date, event_type
ORDER BY event_date DESC, event_count DESC;


-- 2. Regional traffic distribution
--    Expected: us-east ~35%, us-west ~25%, eu-west ~20%
SELECT
    region,
    COUNT(*)                                            AS event_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_total,
    ROUND(AVG(total_price), 2)                          AS avg_order_value,
    ROUND(SUM(total_price), 2)                          AS total_revenue
FROM bronze_order_events
WHERE event_type = 'ORDER_PLACED'
GROUP BY region
ORDER BY event_count DESC;


-- 3. Null / parse failure rate
--    Any row reaching Bronze with a NULL order_id indicates a schema mismatch
--    between producer and consumer — investigate immediately.
SELECT
    COUNT(*)                                        AS total_rows,
    SUM(CASE WHEN order_id       IS NULL THEN 1 END) AS null_order_ids,
    SUM(CASE WHEN customer_id    IS NULL THEN 1 END) AS null_customer_ids,
    SUM(CASE WHEN event_type     IS NULL THEN 1 END) AS null_event_types,
    SUM(CASE WHEN event_timestamp IS NULL THEN 1 END) AS null_timestamps,
    ROUND(SUM(CASE WHEN total_price < 0 THEN 1 END) * 100.0 / COUNT(*), 2) AS pct_negative_price
FROM bronze_order_events;


-- 4. Kafka metadata — partition lag check
--    Uneven partition distribution suggests a partitioning key imbalance.
SELECT
    kafka_partition,
    COUNT(*)           AS row_count,
    MIN(kafka_offset)  AS min_offset,
    MAX(kafka_offset)  AS max_offset
FROM bronze_order_events
GROUP BY kafka_partition
ORDER BY kafka_partition;
