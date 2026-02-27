# NYC Taxi Lakehouse — Medallion Architecture on Databricks

> **Hypothetical Showcase:** Demonstrates a production-grade lakehouse pattern using the publicly available NYC TLC Yellow Taxi dataset. All infrastructure references (cluster configs, volumes, credentials) are illustrative. The dataset is real and freely downloadable — you can run this pipeline yourself on any Databricks workspace with Unity Catalog enabled.

---

## What this is

A complete **Bronze → Silver → Gold medallion pipeline** built on Databricks, Delta Lake, and Unity Catalog. It ingests ~3M rows/month of NYC Yellow Taxi trip data, cleans and validates them, and produces three analytics-ready gold tables covering:

- Trip demand by pickup zone
- Hourly and day-of-week demand patterns
- Fare and tip analysis by payment type

Two deployment options are included:

| Option | File | Best for |
|--------|------|----------|
| **Notebook pipeline** | `notebooks/01–04` | Step-by-step walkthroughs, learning the pattern |
| **Delta Live Tables** | `dlt/taxi_pipeline.py` | Production deployments, automatic lineage + DQ |

---

## Dataset

**NYC TLC Yellow Taxi Trip Records**
- Source: [NYC Open Data / TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- Format: Monthly Parquet files, ~50–150 MB each, ~3M rows/month
- License: Free — no API key required
- Download URLs: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{MM}.parquet`

The setup notebook (`01_setup.py`) downloads the data for you. Choose 1–12 months.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Source: TLC Public S3 (Parquet files, monthly)                         │
└─────────────────────────────┬───────────────────────────────────────────┘
                              │  wget / requests
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Unity Catalog Volume  /Volumes/taxi_lakehouse/bronze/raw_files/        │
│  yellow_tripdata_2024-01.parquet … yellow_tripdata_2024-12.parquet      │
└─────────────────────────────┬───────────────────────────────────────────┘
                              │  Auto Loader (cloudFiles)
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  BRONZE  taxi_lakehouse.bronze.raw_trips                                │
│  ─ Source-faithful, no transformations                                  │
│  ─ _source_file + _ingested_at metadata columns                        │
│  ─ Change Data Feed enabled (feeds incremental silver reads)            │
└─────────────────────────────┬───────────────────────────────────────────┘
                              │  PySpark transforms + DQ checks
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  SILVER  taxi_lakehouse.silver.trips                                    │
│  ─ SHA2 surrogate key (vendor + pickup_datetime + pu_location)          │
│  ─ Decoded enums (payment type, rate code)                              │
│  ─ Derived fields: duration, speed, tip %, airport flag                 │
│  ─ 10 DQ rules — failed rows flagged, kept for investigation            │
│  ─ Partitioned by pickup_date, Z-ORDERed by pu_location_id              │
│  ─ MERGE upsert — safe to re-run                                        │
└──────────────┬──────────────┬──────────────┬───────────────────────────┘
               │              │              │
               ▼              ▼              ▼
┌──────────────────┐  ┌────────────────┐  ┌───────────────────────────┐
│  GOLD            │  │  GOLD          │  │  GOLD                     │
│  trips_by_zone   │  │  hourly_       │  │  fare_analysis            │
│                  │  │  patterns      │  │                           │
│  Daily grain     │  │  Hour × DOW    │  │  payment_type × rate_code │
│  pu_zone +       │  │  grain across  │  │  avg/median/P95 fare      │
│  pu_borough      │  │  full dataset  │  │  tip % by payment method  │
│  enriched from   │  │  weekday vs    │  │  % of total trips         │
│  zone lookup     │  │  weekend split │  │                           │
└──────────────────┘  └────────────────┘  └───────────────────────────┘
```

---

## Key Technical Decisions

### Auto Loader over manual COPY INTO

```python
spark.readStream
     .format("cloudFiles")
     .option("cloudFiles.format", "parquet")
     .option("cloudFiles.schemaLocation", checkpoint_path)
     .schema(TRIPS_SCHEMA)
     .load(source_path)
```

Auto Loader tracks which files have been ingested via a checkpoint, so new monthly files added to the Volume are automatically picked up without reprocessing all 12 months. `Trigger.AvailableNow` runs it as a batch — all pending files processed, then stops.

### Explicit schema (no inference)

Schema inference reads a sample of every file — expensive at scale. An explicit `StructType` is declared once and reused. This also prevents silent schema drift from breaking the pipeline if the TLC changes column types.

### Surrogate key via SHA2

```python
F.sha2(F.concat_ws("|", vendor_id, pickup_datetime, pu_location_id), 256)
```

The natural key `(VendorID, tpep_pickup_datetime, PULocationID)` deduplicates across vendor sources if the same trip is reported by multiple providers. The SHA2 hash is deterministic — replaying the same bronze data always produces the same `trip_id`.

### Soft DQ failures (flag, don't drop)

Silver keeps all rows but adds `dq_valid` and `dq_flags` columns. Analysts can choose to include or exclude flagged data. Gold tables filter `dq_valid = true`. The DQ failure breakdown is queryable:

```sql
SELECT flag, COUNT(*) AS trip_count
FROM taxi_lakehouse.silver.trips
LATERAL VIEW explode(dq_flags) t AS flag
WHERE NOT dq_valid
GROUP BY 1 ORDER BY 2 DESC;
```

### Z-ORDER for query acceleration

```sql
OPTIMIZE taxi_lakehouse.silver.trips
ZORDER BY (pu_location_id, pickup_datetime);
```

Delta's Z-ORDER co-locates rows with the same `pu_location_id` on the same set of files. Queries filtering by location (the most common pattern) skip entire file groups, reducing scan size by 60–80% on a 12-month dataset.

### Delta time travel

Gold tables use `OVERWRITE` mode — each pipeline run creates a new Delta version. Historical snapshots are always available:

```sql
-- Compare this week's top zones to last month's
SELECT * FROM taxi_lakehouse.gold.trips_by_zone
TIMESTAMP AS OF CURRENT_TIMESTAMP - INTERVAL 30 DAYS
WHERE pu_borough = 'Manhattan'
ORDER BY trip_count DESC
LIMIT 10;
```

### DLT expectations (quarantine pattern)

In the Delta Live Tables version, `@dlt.expect_all_or_drop` silently quarantines rows violating DQ rules to a separate system table. Violations are tracked in the DLT event log without stopping the pipeline or corrupting the silver table:

```python
@dlt.expect_all_or_drop({
    "valid_duration":     "trip_duration_minutes > 0 AND trip_duration_minutes <= 1440",
    "valid_distance":     "trip_distance_miles >= 0 AND trip_distance_miles <= 500",
    "valid_fare":         "fare_amount >= 0 AND fare_amount <= 1000",
    "valid_location_ids": "pu_location_id BETWEEN 1 AND 265",
})
def trips():
    ...
```

---

## Project Structure

```
lakehouse-medallion/
│
├── sql/
│   └── catalog_setup.sql          Unity Catalog setup — catalog, schemas, table DDL
│
├── notebooks/
│   ├── 01_setup.py                Download TLC data → Unity Catalog Volume
│   ├── 02_bronze_ingest.py        Auto Loader → bronze.raw_trips Delta table
│   ├── 03_silver_transform.py     Clean + validate → silver.trips (MERGE upsert)
│   └── 04_gold_aggregations.py    Build three gold analytics tables
│
├── dlt/
│   └── taxi_pipeline.py           Single DLT pipeline covering all three layers
│
└── workflows/
    └── taxi_lakehouse_job.yml     Databricks Asset Bundle — scheduled Workflow config
```

---

## How to Run It

### Prerequisites

- Databricks workspace with **Unity Catalog** enabled (any tier, Community Edition works)
- Cluster with **Databricks Runtime 14.3+** (or 15.4 Photon for best performance)
- ~500 MB free storage in a Unity Catalog Volume

### Steps

**1. Create the catalog, schemas, and tables**

Open `sql/catalog_setup.sql` in a Databricks SQL editor or notebook and run it. This creates:
- `taxi_lakehouse` catalog
- `bronze`, `silver`, `gold` schemas
- All table DDL with correct types and properties

**2. Configure a Volume**

In Unity Catalog, create an external or managed volume at:
```
taxi_lakehouse.bronze.raw_files
```

Or update the `volume_path` widget in each notebook to point to any writable path (`/tmp` works for testing).

**3. Run the setup notebook**

Open `notebooks/01_setup.py` on a cluster. Set `months_to_download` to 1–12. The notebook downloads the monthly Parquet files and the zone lookup CSV directly from the TLC's public CDN — no credentials needed.

**4. Run the pipeline**

**Option A — Notebook by notebook (recommended for first-time exploration):**
```
01_setup.py → 02_bronze_ingest.py → 03_silver_transform.py → 04_gold_aggregations.py
```

**Option B — Delta Live Tables (production pattern):**
1. In Databricks UI → Delta Live Tables → Create Pipeline
2. Set Source Code: `/path/to/dlt/taxi_pipeline.py`
3. Set Target: `taxi_lakehouse`
4. Set Pipeline mode: Triggered
5. Add configuration: `{"volume_path": "/Volumes/taxi_lakehouse/bronze/raw_files"}`
6. Click Start

**Option C — Scheduled Workflow (ongoing ingestion):**
```bash
pip install databricks-cli
databricks bundle deploy
databricks bundle run taxi_lakehouse_pipeline
```

---

## Sample Output

**Top 10 pickup zones (1 month of data):**

| Zone | Borough | Trips | Avg Fare |
|------|---------|-------|----------|
| Midtown Center | Manhattan | 187,429 | $16.40 |
| Upper East Side North | Manhattan | 143,210 | $14.20 |
| Upper East Side South | Manhattan | 138,905 | $13.80 |
| JFK Airport | Queens | 124,330 | $58.90 |
| Times Sq/Theatre District | Manhattan | 118,752 | $15.10 |
| Penn Station/Madison Sq W | Manhattan | 112,441 | $17.30 |
| LaGuardia Airport | Queens | 98,213 | $31.50 |
| Murray Hill | Manhattan | 91,847 | $15.60 |
| Lenox Hill West | Manhattan | 89,342 | $14.90 |
| Lincoln Square East | Manhattan | 82,110 | $15.20 |

**Tip rate by payment type:**

| Payment Type | Trips | Avg Fare | Avg Tip % |
|-------------|-------|----------|-----------|
| Credit Card | 2,134,819 | $16.80 | 22.4% |
| Cash | 891,204 | $14.20 | 0.0% |
| No Charge | 12,330 | $0.00 | 0.0% |
| Dispute | 4,112 | $18.40 | 0.0% |

*(Values approximate — actual numbers vary by month)*

---

## Tech Stack

`Databricks` · `Delta Lake` · `PySpark` · `Auto Loader` · `Delta Live Tables` · `Unity Catalog` · `Python`
