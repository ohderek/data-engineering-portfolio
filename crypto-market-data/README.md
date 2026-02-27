# CoinMarketCap → Snowflake

> **Hypothetical Showcase:** Demonstrates REST API scraping, response normalisation, Parquet serialisation, and idempotent Snowflake ingestion. Uses the public CoinMarketCap Pro API. All credentials are environment-variable driven — no secrets in code.

---

## What it does

Pulls live cryptocurrency market data from the [CoinMarketCap Pro API](https://coinmarketcap.com/api/) and loads it into Snowflake in two datasets:

| Dataset | API Endpoint | What's captured |
|---------|-------------|-----------------|
| **Coin listings** | `/v1/cryptocurrency/listings/latest` | Price, 24h volume, market cap, % changes (1h/24h/7d/30d), circulating supply, CMC rank — for the top N coins |
| **Global metrics** | `/v1/global-metrics/quotes/latest` | Total crypto market cap, BTC/ETH dominance, active coins and exchanges |

Both datasets are loaded as timestamped snapshots, so running the pipeline multiple times per day builds a historical series you can trend over time.

---

## Pipeline Flow

```
CoinMarketCap Pro API
        │
        │  GET /v1/cryptocurrency/listings/latest  (paginated, up to 5,000 coins)
        │  GET /v1/global-metrics/quotes/latest    (single snapshot)
        ▼
src/coinmarketcap_client.py
  ├── Paginates using start-offset loop (CMC caps at 5,000 per call)
  ├── Injects API key via X-CMC_PRO_API_KEY header (never in query params)
  ├── Detects 429 rate-limit responses, waits Retry-After seconds
  ├── Retries up to 4× on 5xx with exponential back-off (2s, 4s, 8s, 16s)
  └── Returns typed Python dataclasses (CoinListing, GlobalMetrics)
        │
        ▼
src/transform.py
  ├── Maps dataclasses → explicit PyArrow schema (float64, int32, bool, string)
  ├── Enforces schema at write time — type mismatches raise before hitting Snowflake
  └── Serialises to Parquet with Snappy compression → in-memory bytes buffer
        │
        ▼
src/snowflake_loader.py
  ├── PUT Parquet file to internal @crypto_stage
  ├── COPY INTO staging table (MATCH_BY_COLUMN_NAME, PURGE=TRUE)
  ├── MERGE INTO target table on (id, fetched_at) — idempotent upsert
  └── TRUNCATE staging table → clean slate for next run
        │
        ▼
CRYPTO.RAW.CMC_LISTINGS          one row per (coin, snapshot)
CRYPTO.RAW.CMC_GLOBAL_METRICS    one row per snapshot
        │
        ▼
CRYPTO.ANALYTICS.*               pre-aggregated views for dashboards
```

---

## Project Structure

```
crypto-market-data/
├── main.py                       Orchestration entry point — runs extract → transform → load
│                                 Accepts --limit and --no-global-metrics CLI flags
├── requirements.txt              httpx, pyarrow, snowflake-connector-python
├── src/
│   ├── coinmarketcap_client.py   API client
│   │     • CoinMarketCapClient class
│   │     • listings_pages() generator — yields one page of CoinListing objects per API call
│   │     • global_metrics() — returns a single GlobalMetrics snapshot
│   │     • _get() — shared HTTP logic: headers, retry, back-off
│   │     • CoinListing / GlobalMetrics dataclasses — typed, serialisable with asdict()
│   │
│   ├── transform.py              Serialisation layer
│   │     • LISTINGS_SCHEMA / GLOBAL_METRICS_SCHEMA — explicit PyArrow schemas
│   │     • listings_to_parquet() — list[CoinListing] → Parquet bytes
│   │     • global_metrics_to_parquet() — GlobalMetrics → Parquet bytes
│   │
│   └── snowflake_loader.py       Snowflake ingest layer
│         • SnowflakeLoader class (context manager — auto-closes connection)
│         • load_listings() — upserts coin snapshot data
│         • load_global_metrics() — upserts global metrics snapshot
│         • _stage_and_merge() — shared PUT → COPY INTO → MERGE → TRUNCATE logic
│
└── sql/
    └── create_tables.sql         Run once to set up Snowflake objects
          • @crypto_stage (internal stage)
          • CMC_LISTINGS + CMC_LISTINGS_STAGE tables
          • CMC_GLOBAL_METRICS + CMC_GLOBAL_METRICS_STAGE tables
          • Four analytics views (see below)
```

---

## How to Implement

### 1. Prerequisites

- Python 3.11+
- A [CoinMarketCap Pro API key](https://coinmarketcap.com/api/) (free tier gives 10,000 credits/month — enough for daily top-500 runs)
- A Snowflake account with a user configured for key-pair authentication

### 2. Snowflake setup

Run `sql/create_tables.sql` once against your Snowflake account to create the database, schemas, stage, tables, and analytics views:

```sql
-- In Snowflake worksheet or SnowSQL:
USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS CRYPTO;
CREATE SCHEMA IF NOT EXISTS CRYPTO.RAW;
CREATE SCHEMA IF NOT EXISTS CRYPTO.ANALYTICS;

-- Then run the full contents of sql/create_tables.sql
```

### 3. Snowflake key-pair authentication

```bash
# Generate RSA key pair (no passphrase for service accounts)
openssl genrsa -out rsa_key.pem 2048
openssl rsa -in rsa_key.pem -pubout -out rsa_key.pub.pem
openssl pkcs8 -topk8 -inform PEM -outform DER -nocrypt \
  -in rsa_key.pem -out rsa_key.p8

# Assign public key to your Snowflake user
ALTER USER etl_user SET RSA_PUBLIC_KEY='<contents of rsa_key.pub.pem>';
```

### 4. Environment variables

```bash
export CMC_API_KEY="your-coinmarketcap-pro-api-key"
export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"       # from your Snowflake URL
export SNOWFLAKE_USER="etl_user"
export SNOWFLAKE_PRIVATE_KEY_PATH="/path/to/rsa_key.p8"
export SNOWFLAKE_ROLE="TRANSFORMER"                 # optional, default: TRANSFORMER
export SNOWFLAKE_WAREHOUSE="CRYPTO_WH"              # optional, default: CRYPTO_WH
export SNOWFLAKE_DATABASE="CRYPTO"                  # optional, default: CRYPTO
export SNOWFLAKE_SCHEMA="RAW"                       # optional, default: RAW
```

### 5. Install and run

```bash
pip install -r requirements.txt

# Fetch top 500 coins + global metrics (default)
python main.py

# Fetch top 5,000 coins
python main.py --limit 5000

# Listings only, skip global metrics
python main.py --no-global-metrics
```

### 6. Schedule it

Wire into any scheduler — the pipeline is fully idempotent (safe to re-run):

```python
# Airflow DAG example
from airflow.operators.bash import BashOperator

fetch_crypto = BashOperator(
    task_id="fetch_coinmarketcap",
    bash_command="python /opt/crypto-market-data/main.py --limit 500",
    env={"CMC_API_KEY": "{{ var.value.cmc_api_key }}", ...},
    dag=dag,
)
```

---

## Snowflake Analytics Layer

After loading, four views make the data immediately queryable for dashboards:

| View | Grain | Description |
|------|-------|-------------|
| `ANALYTICS.DAILY_COIN_PRICES` | (coin, date) | Latest snapshot per coin per calendar day — `QUALIFY ROW_NUMBER()` dedup picks the last fetch |
| `ANALYTICS.CURRENT_TOP_10` | coin | Top 10 coins by CMC rank at the most recent fetch timestamp |
| `ANALYTICS.PRICE_HISTORY_7D` | (coin, date) | Daily OHLC + max volume per coin for the trailing 7 days |
| `ANALYTICS.BTC_DOMINANCE_TREND` | date | Daily avg BTC/ETH dominance, total market cap (in trillions), 24h volume |

```sql
-- 7-day price history for Bitcoin
SELECT price_date, avg_price_usd, high_price_usd, low_price_usd, max_volume_usd
FROM CRYPTO.ANALYTICS.PRICE_HISTORY_7D
WHERE SYMBOL = 'BTC'
ORDER BY price_date;

-- Current market snapshot: top 10
SELECT rank, symbol, price_usd, market_cap_usd, change_24h_pct
FROM CRYPTO.ANALYTICS.CURRENT_TOP_10;

-- Bitcoin dominance over the last 30 days
SELECT metric_date, avg_btc_dominance_pct, avg_total_mcap_trillion_usd
FROM CRYPTO.ANALYTICS.BTC_DOMINANCE_TREND
WHERE metric_date >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY metric_date;
```

---

## Key Design Decisions

| Decision | Reasoning |
|----------|-----------|
| `start`-offset pagination | CMC's listings endpoint caps at 5,000 rows per call. The loop increments `start` by `batch_size` until `limit` coins are fetched or the API returns fewer rows than requested. |
| `X-CMC_PRO_API_KEY` header (not query param) | CMC recommends header-based auth — query params can appear in server logs and browser history. |
| Respect `Retry-After` on 429 | CMC measures usage in credits. When rate-limited, the response includes how long to wait — sleeping exactly that long avoids burning retry credits unnecessarily. |
| Explicit PyArrow schema | Without a schema, `pa.Table.from_pylist` infers types from the first row. A `None` price on coin #1 would infer `null` type, causing a schema mismatch on all subsequent rows. |
| Parquet + Snowflake COPY INTO | Snowflake's COPY INTO is the fastest ingest path — it parallelises the load server-side. Parquet's columnar encoding also compresses float-heavy price data better than CSV. |
| MERGE on `(id, fetched_at)` | Allows multiple snapshots per coin per day (hourly runs) while preventing duplicates if the same run executes twice. |
| `CLUSTER BY DATE(FETCHED_AT)` | Micro-partition pruning for date-range queries. A `WHERE fetched_at >= ...` on a clustered table scans only relevant partitions instead of the full table. |

---

## Tech Stack

`Python 3.11` · `httpx` · `PyArrow` · `Snowflake` · `CoinMarketCap Pro API`
