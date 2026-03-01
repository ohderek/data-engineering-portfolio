-- gold_revenue.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Real-time revenue queries against the Gold window aggregation table.
-- Windows are 5-minute tumbling; the table updates every 30 seconds.
--
-- Load in PySpark before running:
--   spark.read.format("delta").load("./delta/gold/order_revenue_windows") \
--        .createOrReplaceTempView("gold_order_revenue_windows")
-- ─────────────────────────────────────────────────────────────────────────────


-- 1. Last 30 minutes — revenue by region, sorted by most recent window first
SELECT
    window_start,
    window_end,
    region,
    order_count,
    total_revenue,
    avg_order_value,
    unique_customers,
    -- Revenue share within each 5-minute window
    ROUND(
        total_revenue * 100.0 / SUM(total_revenue) OVER (PARTITION BY window_start),
        1
    ) AS region_revenue_pct
FROM gold_order_revenue_windows
WHERE window_start >= CURRENT_TIMESTAMP - INTERVAL 30 MINUTES
ORDER BY window_start DESC, total_revenue DESC;


-- 2. Revenue by product category over the last hour
SELECT
    product_category,
    SUM(order_count)      AS total_orders,
    SUM(total_units_sold) AS total_units,
    ROUND(SUM(total_revenue), 2)      AS total_revenue,
    ROUND(AVG(avg_order_value), 2)    AS avg_order_value,
    SUM(unique_customers)             AS unique_customers
FROM gold_order_revenue_windows
WHERE window_start >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
GROUP BY product_category
ORDER BY total_revenue DESC;


-- 3. Revenue trend — 5-minute buckets for the last 2 hours
--    Use this to visualise volume spikes and detect anomalies.
SELECT
    window_start,
    SUM(order_count)             AS total_orders,
    ROUND(SUM(total_revenue), 2) AS total_revenue,
    SUM(unique_customers)        AS unique_customers,
    -- Simple moving average over the last 3 windows (15 min)
    ROUND(
        AVG(SUM(total_revenue)) OVER (
            ORDER BY window_start
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS revenue_15min_ma
FROM gold_order_revenue_windows
WHERE window_start >= CURRENT_TIMESTAMP - INTERVAL 2 HOURS
GROUP BY window_start
ORDER BY window_start;
