# GitHub Insights

> **Hypothetical Showcase:** Demonstrates two independent approaches to building an engineering velocity and DORA metrics platform on GitHub data. Each approach covers raw ingestion through to analytics-ready marts. All company names, org identifiers, and credentials are fully anonymised. No proprietary data is included.

---

## Two Approaches to Raw Ingestion

The biggest architectural decision in any GitHub analytics platform is **where the raw data comes from**. This project implements both options end-to-end, so you can pick the right one for your stack.

```
┌────────────────────────────────┐     ┌────────────────────────────────────┐
│  APPROACH 1 — FIVETRAN         │     │  APPROACH 2 — CUSTOM PIPELINE      │
│                                │     │                                    │
│  Fivetran manages ingestion.   │     │  You own the ingestion code.       │
│  Configure a connector per     │     │  A Prefect flow calls the GitHub   │
│  GitHub org in the UI. No      │     │  REST API directly, transforms to  │
│  ingestion code to write.      │     │  Parquet, and COPY+MERGEs into     │
│                                │     │  Snowflake.                        │
│  Raw tables:                   │     │                                    │
│  FIVETRAN.GITHUB_ORG_A.*       │     │  Raw tables:                       │
│  (named by Fivetran)           │     │  GITHUB_PIPELINES.RAW_TABLES.*     │
│                                │     │  (named by you)                    │
│  → approach-1-fivetran/        │     │                                    │
└────────────────────────────────┘     │  → approach-2-custom-pipeline/     │
                                       └────────────────────────────────────┘
                  │                                     │
                  └─────────────────┬───────────────────┘
                                    │
                     Both approaches converge here:
                     raw tables are structured the same,
                     all downstream code is shared.
                                    │
                        ┌───────────┴───────────┐
                        │                       │
                   sql-jobs/            dbt-github-insights/
                   (raw SQL ETL)        (dbt transformation)
```

**→ Not sure which to use?** Read the [comparison table](#approach-comparison) below.

---

## Project Structure

```
github-insights/
│
├── approach-1-fivetran/               ← Start here if using Fivetran
│   ├── README.md                        Setup guide + Fivetran connector walkthrough
│   └── sql/
│       └── union_connectors.sql         Consolidate multi-org connectors into RAW_TABLES
│                                        (UNION BY NAME + QUALIFY ROW_NUMBER() dedup)
│
├── approach-2-custom-pipeline/        ← Start here if building your own pipeline
│   ├── README.md                        Setup guide + environment config
│   └── prefect_flows/
│       └── github_pr_ingestion_flow.py  Prefect 3.x flow: GitHub API → GCS → Snowflake
│
├── sql-jobs/                          ← Shared — runs after either approach
│   ├── reporting_data_model.sql         BRIDGE_GITHUB_LDAP, FACT_PULL_REQUESTS,
│   │                                    FACT_PR_REVIEW_SUMMARY (HASHDIFF change detection)
│   └── lead_time_to_deploy.sql          7-stage DORA lead time pipeline
│                                        (service registry → SHA match → time fallback → MERGE)
│
└── dbt-github-insights/               ← Shared dbt transformation layer
    ├── dbt_project.yml
    └── models/
        ├── staging/github/
        │   ├── _sources_fivetran.yml    ← Use this with Approach 1
        │   ├── _sources_custom.yml      ← Use this with Approach 2
        │   ├── stg_github__pull_requests.sql
        │   └── stg_github__commit_files.sql
        ├── intermediate/
        │   ├── int_github__pr_lifecycle.sql
        │   └── int_github__lead_time_to_deploy.sql
        └── marts/
            ├── engineering_velocity/
            │   ├── fct_github__pull_requests.sql
            │   ├── fct_github__lead_time_to_deploy.sql
            │   └── dim_github__users.sql
            └── code_quality/
                └── fct_github__commit_files.sql
```

---

## Approach Comparison

| | Approach 1 — Fivetran | Approach 2 — Custom Pipeline |
|---|---|---|
| **Ingestion code** | None — configured in Fivetran UI | ~500 lines of Python (Prefect + httpx + PyArrow) |
| **Raw table names** | `fivetran.github_org_a.pull_request` (Fivetran schema) | `GITHUB_PIPELINES.RAW_TABLES.PULL_REQUEST` (your schema) |
| **Metadata columns** | `_fivetran_synced`, `_fivetran_deleted` | `_ingested_at`, `_source_org`, `_batch_id` |
| **Sync frequency** | Fivetran plan dependent (typically 1–24h) | Any — configurable, can run every 15 min |
| **Multi-org handling** | One connector per org, sharding for large orgs | Loop over orgs in the same flow |
| **Schema evolution** | Fivetran handles API changes automatically | You update the PyArrow schema and retest |
| **Cost model** | Fivetran licensing + MAR charges | Compute cost only |
| **Operational burden** | Very low | You own incident response for the pipeline |
| **Historical backfill** | Automatic | Manual pagination through API history |
| **Best for** | Teams prioritising speed and low ops overhead | Teams wanting cost control or custom schemas |

---

## What This Platform Measures

Regardless of which ingestion approach you use, the downstream models power:

- **DORA Lead Time to Deploy** — first commit → staging → production (hours), with SHA match and time-based fallback
- **DORA Change Frequency** — deployments per day/week by service
- **PR cycle time** — time to first review, time to approval, time to merge (P50/P75/P95)
- **Review coverage** — % of PRs reviewed externally (not self-merged)
- **Code churn** — file-level change volume by language and service, with lock-file noise filtering
- **Team benchmarking** — all metrics sliced by team, function, and org using a SCD Type 2 user dimension

---

## Data Model (ERD)

```
                    DIM_GITHUB__USERS (SCD Type 2)
                       │  one row per (engineer × period)
                       │  is_current = TRUE for latest snapshot
                       │
                       ├──── FACT_GITHUB__PULL_REQUESTS
                       │       one row per PR
                       │       cluster_by: pr_created_date
                       │
                       ├──── FACT_PR_REVIEW_SUMMARY
                       │       aggregated review metrics per PR
                       │       self-reviews excluded
                       │
                       └──── FACT_GITHUB__LEAD_TIME_TO_DEPLOY
                               one row per (PR × service)
                               DORA bucket: elite / high / medium / low

FACT_GITHUB__COMMIT_FILES     one row per (commit_sha, filepath)
                              noise flags: is_lock_file, is_merge_commit
```

**Why SCD Type 2 for users?** Engineers transfer teams. Without it, a PR from when an engineer was on Team A would be attributed to Team B after the move. SCD2 ensures point-in-time team attribution is always accurate.

---

## Shared Downstream: SQL Jobs vs dbt

The `sql-jobs/` and `dbt-github-insights/` layers both implement the same data model — choose one or use both to compare approaches.

**`sql-jobs/` (raw SQL)** — standalone Snowflake scripts, no framework. Good for:
- Running as Snowflake Tasks or Airflow `SnowflakeOperator`
- Understanding the logic without dbt abstraction
- Environments where dbt isn't available

**`dbt-github-insights/` (dbt)** — the same logic with dbt benefits:
- Jinja loops replace duplicated multi-org SQL blocks
- `persist_docs` pushes column descriptions to Snowflake
- Schema tests enforce data contracts (`not_null`, `unique`, `accepted_values`)
- `incremental_predicates` prune micro-partitions on large mart models
- `dbt docs generate` produces a browsable data dictionary

---

## Quick Start

### With Fivetran (Approach 1)

```
1. Configure Fivetran connectors → see approach-1-fivetran/README.md
2. Run approach-1-fivetran/sql/union_connectors.sql
3. Run sql-jobs/reporting_data_model.sql
4. Run sql-jobs/lead_time_to_deploy.sql
   — or —
4. cd dbt-github-insights
   cp models/staging/github/_sources_fivetran.yml models/staging/github/_sources.yml
   dbt deps && dbt run && dbt test
```

### With Custom Pipeline (Approach 2)

```
1. Deploy Prefect flow → see approach-2-custom-pipeline/README.md
2. Run the flow to populate GITHUB_PIPELINES.RAW_TABLES
3. Run sql-jobs/reporting_data_model.sql
4. Run sql-jobs/lead_time_to_deploy.sql
   — or —
4. cd dbt-github-insights
   cp models/staging/github/_sources_custom.yml models/staging/github/_sources.yml
   dbt deps && dbt run && dbt test
```

---

## Tech Stack

`Python 3.11` · `Prefect 3.x` · `dbt Core` · `Snowflake` · `PyArrow` · `GitHub REST API` · `Fivetran` · `GCS`
