# CoinMarketCap → Snowflake

> **Hypothetical Showcase:** Demonstrates REST API scraping, Parquet serialisation, and Snowflake ingestion patterns. Uses the public CoinMarketCap Pro API. No API keys or credentials are included — set `CMC_API_KEY` as an environment variable before running.

## What it does

Pulls cryptocurrency market data from the [CoinMarketCap Pro API](https://coinmarketcap.com/api/) and loads it into Snowflake for analytics:

- **Coin listings** — price, volume, market cap, % changes, supply data for the top N coins by market cap
- **Global market metrics** — total market cap, BTC/ETH dominance, active coins/exchanges

Runs as a standalone Python script — wire it into any scheduler (Airflow, Prefect, cron).

## Flow

```
CoinMarketCap Pro API
        │
        ▼
CoinMarketCapClient          — paginated GET, rate-limit aware, retry + back-off
        │
        ▼
transform.py                 — dataclass → PyArrow Table → Parquet bytes (Snappy)
        │
        ▼
SnowflakeLoader              — PUT to @stage → COPY INTO staging → MERGE → TRUNCATE
        │
        ▼
CRYPTO.RAW.CMC_LISTINGS      — one row per (coin, snapshot)
CRYPTO.RAW.CMC_GLOBAL_METRICS
        │
        ▼
CRYPTO.ANALYTICS.*           — daily prices, top 10, 7-day history, BTC dominance
```

## Key Design Decisions

| Decision | Why |
|----------|-----|
| Pagination via `start` offset | CMC caps at 5,000 coins per request; loop with start offset to fetch all |
| Retry on 429 with `Retry-After` header | CMC rate limits by credit usage; honour the server's back-off window |
| PyArrow + Parquet (Snappy) | Columnar format natively supported by Snowflake COPY INTO; efficient for float-heavy data |
| Stage-and-merge pattern | Idempotent — re-running the pipeline on the same data produces no duplicates |
| `CLUSTER BY DATE(FETCHED_AT)` | Partition pruning on date-range queries (daily price history, trend analysis) |
| Dataclasses for API responses | Type-safe, serialisable; `asdict()` feeds directly into PyArrow `from_pylist` |

## Project Structure

```
crypto-market-data/
├── main.py                       Entry point — orchestrates extract → transform → load
├── requirements.txt
├── src/
│   ├── coinmarketcap_client.py   API client: pagination, retry, response parsing
│   ├── transform.py              Dataclass → PyArrow schema → Parquet bytes
│   └── snowflake_loader.py       PUT → COPY INTO → MERGE → TRUNCATE
└── sql/
    └── create_tables.sql         Snowflake DDL + analytics views
```

## Usage

```bash
pip install -r requirements.txt

export CMC_API_KEY="your-cmc-pro-api-key"
export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"
export SNOWFLAKE_USER="etl_user"
export SNOWFLAKE_PRIVATE_KEY_PATH="/path/to/rsa_key.p8"

# Fetch top 500 coins + global metrics (default)
python main.py

# Fetch top 5,000 coins
python main.py --limit 5000

# Skip global metrics snapshot
python main.py --no-global-metrics
```

## Snowflake Analytics Layer

After loading, four pre-aggregated views are available:

| View | Description |
|------|-------------|
| `ANALYTICS.DAILY_COIN_PRICES` | Latest snapshot per coin per day (QUALIFY dedup) |
| `ANALYTICS.CURRENT_TOP_10` | Top 10 coins by market cap at most recent fetch |
| `ANALYTICS.PRICE_HISTORY_7D` | Daily OHLC + volume for any coin over the past 7 days |
| `ANALYTICS.BTC_DOMINANCE_TREND` | BTC/ETH dominance and total market cap trend by day |

```sql
-- Example: 7-day price history for Bitcoin
SELECT * FROM CRYPTO.ANALYTICS.PRICE_HISTORY_7D
WHERE SYMBOL = 'BTC'
ORDER BY price_date;

-- Example: current market overview
SELECT rank, symbol, price_usd, market_cap_usd, change_24h_pct
FROM CRYPTO.ANALYTICS.CURRENT_TOP_10;
```

## Tech Stack

`Python 3.11` · `httpx` · `PyArrow` · `Snowflake` · `CoinMarketCap Pro API`
