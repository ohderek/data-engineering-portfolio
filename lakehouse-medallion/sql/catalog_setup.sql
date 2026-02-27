-- catalog_setup.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Unity Catalog setup for the NYC Taxi Lakehouse.
-- Run once against a Databricks SQL warehouse or cluster with MANAGE privilege.
--
-- Three-level namespace: catalog → schema → table
--   taxi_lakehouse.bronze.raw_trips
--   taxi_lakehouse.bronze.raw_zones
--   taxi_lakehouse.silver.trips
--   taxi_lakehouse.gold.trips_by_zone
--   taxi_lakehouse.gold.hourly_patterns
--   taxi_lakehouse.gold.fare_analysis
-- ─────────────────────────────────────────────────────────────────────────────


-- ── Catalog ──────────────────────────────────────────────────────────────────

CREATE CATALOG IF NOT EXISTS taxi_lakehouse
  COMMENT 'NYC Yellow Taxi medallion lakehouse — bronze / silver / gold';


-- ── Schemas ──────────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS taxi_lakehouse.bronze
  COMMENT 'Raw ingested data — no transformations, source-faithful';

CREATE SCHEMA IF NOT EXISTS taxi_lakehouse.silver
  COMMENT 'Cleaned, validated, enriched trips — analysis-ready grain';

CREATE SCHEMA IF NOT EXISTS taxi_lakehouse.gold
  COMMENT 'Pre-aggregated analytics marts — BI and reporting layer';


-- ── External Volume (optional — for Auto Loader source path) ─────────────────
-- If landing raw Parquet files in cloud storage before ingestion:
--   CREATE EXTERNAL LOCATION taxi_raw_files
--     URL 's3://your-bucket/taxi-raw/'
--     WITH (STORAGE CREDENTIAL your_credential);
--
--   CREATE VOLUME IF NOT EXISTS taxi_lakehouse.bronze.raw_files
--     LOCATION 's3://your-bucket/taxi-raw/';


-- ── Bronze tables (schema pre-declared for documentation) ────────────────────

CREATE TABLE IF NOT EXISTS taxi_lakehouse.bronze.raw_trips (
  VendorID              BIGINT,
  tpep_pickup_datetime  TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count       DOUBLE,
  trip_distance         DOUBLE,
  RatecodeID            DOUBLE,
  store_and_fwd_flag    STRING,
  PULocationID          BIGINT,
  DOLocationID          BIGINT,
  payment_type          BIGINT,
  fare_amount           DOUBLE,
  extra                 DOUBLE,
  mta_tax               DOUBLE,
  tip_amount            DOUBLE,
  tolls_amount          DOUBLE,
  improvement_surcharge DOUBLE,
  total_amount          DOUBLE,
  congestion_surcharge  DOUBLE,
  airport_fee           DOUBLE,
  -- medallion metadata
  _source_file          STRING  COMMENT 'Source Parquet file name',
  _ingested_at          TIMESTAMP COMMENT 'UTC timestamp of ingestion run'
)
USING DELTA
COMMENT 'Raw NYC Yellow Taxi trip records. One row per trip. No transformations applied.'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
);


CREATE TABLE IF NOT EXISTS taxi_lakehouse.bronze.raw_zones (
  LocationID   INT,
  Borough      STRING,
  Zone         STRING,
  service_zone STRING,
  _ingested_at TIMESTAMP
)
USING DELTA
COMMENT 'NYC Taxi Zone lookup table. 265 zones across 6 boroughs.';


-- ── Silver table ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS taxi_lakehouse.silver.trips (
  trip_id               STRING   COMMENT 'Surrogate key: SHA2(vendor + pickup_datetime + pu_location)',
  vendor_id             INT,
  pickup_datetime       TIMESTAMP,
  dropoff_datetime      TIMESTAMP,
  trip_duration_minutes DOUBLE   COMMENT 'dropoff - pickup in minutes',
  trip_distance_miles   DOUBLE,
  avg_speed_mph         DOUBLE   COMMENT 'trip_distance / (duration / 60)',
  passenger_count       INT,
  pu_location_id        INT,
  do_location_id        INT,
  rate_code             STRING   COMMENT 'Human-readable: Standard / JFK / Newark / etc.',
  payment_type          STRING   COMMENT 'Human-readable: Credit Card / Cash / No Charge / Dispute',
  fare_amount           DOUBLE,
  tip_amount            DOUBLE,
  tolls_amount          DOUBLE,
  total_amount          DOUBLE,
  tip_pct               DOUBLE   COMMENT 'tip_amount / fare_amount — null when fare = 0',
  is_airport_trip       BOOLEAN  COMMENT 'True when RatecodeID = 2 (JFK) or 3 (Newark)',
  -- data quality flags
  dq_valid              BOOLEAN  COMMENT 'False if any hard validation fails',
  dq_flags              ARRAY<STRING> COMMENT 'List of failed DQ rule names',
  -- lineage
  _source_file          STRING,
  _ingested_at          TIMESTAMP
)
USING DELTA
PARTITIONED BY (DATE(pickup_datetime))
COMMENT 'Cleaned and validated trip records. Surrogate key deduplicates across vendor sources.'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.dataSkippingNumIndexedCols' = '8'
);

-- Z-ORDER on most common filter/join columns — run after first OPTIMIZE
-- OPTIMIZE taxi_lakehouse.silver.trips ZORDER BY (pu_location_id, pickup_datetime);


-- ── Gold tables ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS taxi_lakehouse.gold.trips_by_zone (
  pickup_date           DATE,
  pu_location_id        INT,
  pu_zone               STRING,
  pu_borough            STRING,
  trip_count            BIGINT,
  avg_fare              DOUBLE,
  avg_tip_pct           DOUBLE,
  avg_trip_distance     DOUBLE,
  avg_duration_minutes  DOUBLE,
  total_revenue         DOUBLE
)
USING DELTA
COMMENT 'Daily trip aggregations by pickup zone. Grain: date × pickup zone.'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');


CREATE TABLE IF NOT EXISTS taxi_lakehouse.gold.hourly_patterns (
  hour_of_day           INT     COMMENT '0–23',
  day_of_week           STRING  COMMENT 'Monday … Sunday',
  is_weekend            BOOLEAN,
  trip_count            BIGINT,
  avg_fare              DOUBLE,
  avg_duration_minutes  DOUBLE,
  avg_trip_distance     DOUBLE,
  pct_airport_trips     DOUBLE
)
USING DELTA
COMMENT 'Hourly and day-of-week demand patterns across the full dataset.';


CREATE TABLE IF NOT EXISTS taxi_lakehouse.gold.fare_analysis (
  payment_type          STRING,
  rate_code             STRING,
  is_airport_trip       BOOLEAN,
  trip_count            BIGINT,
  avg_fare              DOUBLE,
  avg_tip_pct           DOUBLE,
  median_fare           DOUBLE,
  p95_fare              DOUBLE,
  pct_of_total_trips    DOUBLE
)
USING DELTA
COMMENT 'Fare and tip breakdown by payment type, rate code, and airport flag.';


-- ── Useful queries to verify each layer ──────────────────────────────────────

-- Bronze row count per source file:
--   SELECT _source_file, COUNT(*) AS rows FROM taxi_lakehouse.bronze.raw_trips GROUP BY 1;

-- Silver DQ failure summary:
--   SELECT flag, COUNT(*) AS trip_count
--   FROM taxi_lakehouse.silver.trips
--   LATERAL VIEW explode(dq_flags) t AS flag
--   WHERE NOT dq_valid
--   GROUP BY 1 ORDER BY 2 DESC;

-- Gold: top 10 busiest pickup zones this month:
--   SELECT pu_zone, pu_borough, SUM(trip_count) AS trips
--   FROM taxi_lakehouse.gold.trips_by_zone
--   WHERE pickup_date >= CURRENT_DATE - INTERVAL 30 DAYS
--   GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10;

-- Delta time travel — compare today's zone stats vs. 7 days ago:
--   SELECT * FROM taxi_lakehouse.gold.trips_by_zone
--   TIMESTAMP AS OF CURRENT_TIMESTAMP - INTERVAL 7 DAYS
--   WHERE pu_borough = 'Manhattan'
--   LIMIT 20;
