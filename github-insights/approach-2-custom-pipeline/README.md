# Approach 2 — Custom Ingestion Pipeline

In this approach, **you build and own the GitHub data ingestion pipeline**. A Prefect flow (or Airflow DAG) calls the GitHub REST API directly, transforms responses to Parquet, and loads them into Snowflake raw tables you define. No third-party connector required.

The tradeoff: you write and maintain the ingestion code, handle pagination and rate limiting yourself, and manage schema evolution. In return, you get full control over schema design, sync frequency, filtering, and cost.

---

## How it works

```
GitHub REST API
  GET /orgs/{org}/pulls?state=all&sort=updated&since={lookback}
  GET /repos/{owner}/{repo}/commits
  GET /orgs/{org}/repos
        │
        │  httpx (async HTTP)
        │  Cursor-based pagination via Link header
        ▼
  PyArrow Parquet transformation
  (explicit schema, Snappy compression)
        │
        ▼
  GCS staging bucket
  gs://your-bucket/github/pull_request/org={org}/date={date}/
        │
        ▼
  Snowflake COPY INTO staging table
        │
        ▼
  MERGE INTO target raw table
  (idempotent — safe to re-run)
        │
        ▼
  GITHUB_PIPELINES.RAW_TABLES.*
```

---

## Files

### [`prefect_flows/github_pr_ingestion_flow.py`](./prefect_flows/github_pr_ingestion_flow.py)

A Prefect 3.x flow that ingests pull request data from one or more GitHub orgs.

**Key design decisions:**
- GitHub API token loaded from **GCP Secret Manager** via `GcpSecret.load()` — the token never touches environment variables or hardcoded config
- Pagination uses the `Link: <url>; rel="next"` response header (cursor-based) — GitHub deprecates offset pagination for orgs with large PR volumes
- Each batch is written to Parquet using **PyArrow with an explicit schema** — prevents type inference failures on null-heavy fields
- GCS path is partitioned by `org=` and `date=` for efficient incremental loads
- Snowflake load uses **COPY INTO staging → MERGE INTO target** — fully idempotent, no duplicate rows if re-run

---

## Raw Table Schema

The custom pipeline loads into your own schema, with naming conventions you control:

```
GITHUB_PIPELINES.RAW_TABLES
├── PULL_REQUEST              one row per PR per org
├── PULL_REQUEST_REVIEW       code review submissions
├── COMMIT_FILE               file-level diff records
└── USER_LOOKUP               GitHub login → LDAP identity mapping
```

Column names follow `UPPER_SNAKE_CASE` Snowflake convention. Custom metadata columns added by the pipeline:

- `_INGESTED_AT` — UTC timestamp when this row was loaded (equivalent to Fivetran's `_FIVETRAN_SYNCED`)
- `_SOURCE_ORG` — the GitHub org this record came from
- `_BATCH_ID` — UUID of the Prefect flow run that loaded this row (useful for debugging)

---

## Setup Steps

**1. Install dependencies**

```bash
pip install prefect prefect-gcp httpx pyarrow snowflake-connector-python pydantic-settings
```

**2. Create Prefect blocks (run once)**

```python
from prefect_gcp import GcsBucket, GcpSecret

# GCS bucket for Parquet staging
GcsBucket(bucket="your-data-bucket").save("gcs-data-bucket")

# GitHub PAT (stored in GCP Secret Manager, not in code)
# Create the secret in GCP first: gcloud secrets create github-api-token --data-file=token.txt
GcpSecret(secret_name="github-api-token", project="your-gcp-project").save("github-api-token")
```

**3. Configure orgs and environment**

```bash
export GITHUB_ORGS='["my-org-1", "my-org-2"]'
export DEPLOYMENT_ENV="prod"
export SNOWFLAKE_ACCOUNT="your-account.snowflakecomputing.com"
export SNOWFLAKE_USER="svc_prefect"
export SNOWFLAKE_WAREHOUSE="ETL_WH"
export SNOWFLAKE_DATABASE="GITHUB_PIPELINES"
export SNOWFLAKE_SCHEMA="RAW_TABLES"
# Snowflake private key path (key-pair auth — no passwords in environment)
export SNOWFLAKE_PRIVATE_KEY_PATH="~/.ssh/snowflake_key.p8"
```

**4. Run the flow**

```bash
# Ad-hoc run
python prefect_flows/github_pr_ingestion_flow.py

# Deploy to Prefect Cloud with a schedule
prefect deploy --name github-pr-ingestion --cron "0 */6 * * *"
```

**5. Run shared downstream layers**

Once `RAW_TABLES` is populated, the downstream layers are identical to Approach 1:

```
approach-2-custom-pipeline/prefect_flows/github_pr_ingestion_flow.py   ← you are here
        ↓
sql-jobs/reporting_data_model.sql
        ↓
sql-jobs/lead_time_to_deploy.sql
        ↓
dbt-github-insights/  (dbt transformation layer)
```

For dbt, use the custom pipeline source file:
```bash
cd dbt-github-insights
# rename _sources_custom.yml → _sources.yml before running
# or use a dbt profile var to select the source
dbt run
```

---

## dbt Sources (Custom Pipeline)

[`../dbt-github-insights/models/staging/github/_sources_custom.yml`](../dbt-github-insights/models/staging/github/_sources_custom.yml)

Key differences from the Fivetran source file:
- `database: GITHUB_PIPELINES`, `schema: RAW_TABLES` — your own schema, not Fivetran's
- `loaded_at_field: _ingested_at` — the custom metadata column added by the pipeline
- Table names are `UPPER_SNAKE_CASE` (Snowflake default for unquoted identifiers)
- No `_fivetran_deleted` or `_fivetran_synced` columns — your pipeline uses `_ingested_at` and `_batch_id` instead

---

## Pros and Cons

| | Custom Pipeline Approach |
|---|---|
| **Setup time** | Hours — write, test, and deploy ingestion code |
| **Maintenance** | You own it — GitHub API changes require code updates |
| **Schema control** | Full — you design the table names, types, and columns |
| **Sync frequency** | Any — run every 15 minutes if needed |
| **Cost** | Compute only — no per-row licensing fee |
| **Multi-org** | Loop over orgs in the same flow — no extra connectors |
| **Historical backfill** | Manual — paginate back through the full API history |
| **Best for** | Teams that want cost control, custom schemas, or very frequent syncs |
