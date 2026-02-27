# Approach 1 — Fivetran as the GitHub Data Source

In this approach, **Fivetran manages all raw data ingestion from the GitHub API**. You configure one Fivetran connector per GitHub organisation in the Fivetran UI — no ingestion code to write or maintain. Fivetran handles pagination, rate limiting, incremental syncs, and schema management automatically.

The tradeoff: you're dependent on Fivetran's connector schema and sync schedule, and the raw tables are named and structured by Fivetran, not you.

---

## How Fivetran works with GitHub

When you connect a GitHub org in Fivetran, it creates a schema in your Snowflake database (e.g. `FIVETRAN.GITHUB_ORG_A`) and populates it with tables that mirror GitHub's REST API object model:

```
FIVETRAN.GITHUB_ORG_A
├── PULL_REQUEST              one row per PR
├── PULL_REQUEST_REVIEW       code review submissions
├── PULL_REQUEST_REVIEW_COMMENT  inline review comments
├── ISSUE_COMMENT             PR conversation comments
├── COMMIT                    commits pushed to the repo
├── COMMIT_FILE               file-level diff records per commit
├── REPOSITORY                repo metadata
├── LABEL                     PR and issue labels
└── PULL_REQUEST_LABEL        bridge: PRs ↔ labels
```

Every table includes two Fivetran metadata columns:
- `_FIVETRAN_SYNCED` — UTC timestamp of the last sync for that row (used for freshness checks and incremental loads)
- `_FIVETRAN_DELETED` — boolean flag set to `true` when Fivetran detects a soft-deleted record

---

## Multi-Org / Multi-Connector Pattern

Large engineering organisations often run multiple GitHub orgs (e.g. a main product org + a separate data/infra org). Fivetran creates **one schema per connector**, so multiple orgs means multiple schemas:

```
FIVETRAN.GITHUB_ORG_A_1     -- large org, connector shard 1
FIVETRAN.GITHUB_ORG_A_2     -- large org, connector shard 2
FIVETRAN.GITHUB_ORG_A_3     -- large org, connector shard 3
FIVETRAN.GITHUB_ORG_A_4     -- large org, connector shard 4
FIVETRAN.GITHUB_ORG_A_5     -- large org, connector shard 5
FIVETRAN.GITHUB_ORG_B       -- second org, single connector
```

Very large orgs (10,000+ repos) require sharding a single GitHub org across multiple connectors because of GitHub API rate limits per token.

The [`union_connectors.sql`](./sql/union_connectors.sql) script handles consolidation:
- `UNION BY NAME` merges all connector schemas (Snowflake-specific — matches by column name, not position, making it safe across slightly different connector versions)
- `QUALIFY ROW_NUMBER()` deduplicates records that appear in multiple shards
- A `CONNECTOR` column is preserved for lineage and debugging

---

## Setup Steps

**1. Create a Fivetran GitHub connector**

In the Fivetran dashboard:
- Source: `GitHub`
- Destination: your Snowflake account
- Destination schema prefix: e.g. `GITHUB_ORG_A`
- Grant a GitHub PAT with `repo` and `read:org` scopes

For each additional org, repeat — one connector per org.

**2. Let Fivetran complete its initial sync**

The initial historical backfill can take several hours for large orgs. Monitor progress in the Fivetran dashboard.

**3. Run the connector union script**

Once all connectors have synced, run [`sql/union_connectors.sql`](./sql/union_connectors.sql) to consolidate all connector schemas into unified `RAW_TABLES`:

```sql
-- Edit the connector list at the top to match your actual Fivetran schema names
-- Then run against your Snowflake account:
<run approach-1-fivetran/sql/union_connectors.sql>
```

**4. Run shared downstream layers**

The `sql-jobs/` and `dbt-github-insights/` layers in the parent directory are shared across both approaches. Once `RAW_TABLES` is populated, continue from there:

```
approach-1-fivetran/sql/union_connectors.sql   ← you are here
        ↓
sql-jobs/reporting_data_model.sql
        ↓
sql-jobs/lead_time_to_deploy.sql
        ↓
dbt-github-insights/  (dbt transformation layer)
```

For dbt, use the Fivetran source file:
```bash
cd dbt-github-insights
dbt run --vars '{"source_type": "fivetran"}'
# or rename _sources_fivetran.yml → _sources.yml before running
```

---

## dbt Sources (Fivetran)

[`../dbt-github-insights/models/staging/github/_sources_fivetran.yml`](../dbt-github-insights/models/staging/github/_sources_fivetran.yml)

Key differences from the custom pipeline source file:
- `database` points to `FIVETRAN` (or your Fivetran destination database)
- `loaded_at_field: _fivetran_synced` — Fivetran adds this to every table
- Freshness thresholds based on your Fivetran sync frequency (typically hourly or daily)
- Table names are lowercase and match Fivetran's GitHub connector schema exactly
- `_fivetran_deleted` flag available for soft-delete filtering

---

## Pros and Cons

| | Fivetran Approach |
|---|---|
| **Setup time** | Minutes — configure in UI, no code |
| **Maintenance** | Near-zero — Fivetran handles schema changes, API updates |
| **Schema control** | Low — Fivetran decides table names and column types |
| **Sync frequency** | Typically 1–24 hours (plan dependent) |
| **Cost** | Fivetran licensing + MAR (monthly active rows) charges |
| **Multi-org** | One connector per org; sharding needed for very large orgs |
| **Historical backfill** | Automatic — Fivetran backfills all available history |
| **Best for** | Teams that want minimal operational overhead on ingestion |
