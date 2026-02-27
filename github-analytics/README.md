# GitHub Pipelines

> **Hypothetical Showcase:** This project demonstrates two approaches to building an engineering velocity and DORA metrics platform on top of GitHub data — raw SQL ETL jobs and a full dbt project. The data model is based on a real ERD I designed from scratch. All company names, internal tool references, and credentials are fully anonymised. No proprietary data is included.

## Data Model (ERD)

The GitHub Pipelines schema tracks the full engineering delivery lifecycle:

```
                    DIM_USER ────────────┐
                       │                 │
                       │            FACT_PR_REVIEWS
                       │            FACT_PR_REVIEW_COMMENTS
                       │
FACT_PULL_REQUESTS ────┼──── FACT_PR_TIMES
        │              │
        │         BRIDGE_PR_LABELS ──── DIM_LABELS
        │
FACT_COMMIT_FILES
FACT_REPO_STATS ──── DIM_REPOSITORY
LEAD_TIME_TO_DEPLOY
```

**Key metrics this model powers:**
- **DORA metrics**: Lead Time to Deploy, Deployment Frequency, Change Failure Rate, MTTR
- **PR velocity**: Time to first review, time to approve, time to merge
- **Code quality**: Review coverage, self-review rate, noisy commit detection
- **Team benchmarking**: Rolling P50/P75/P95 metrics by team, org, and function

---

## Two Approaches: Raw SQL Jobs vs dbt

The same data model is implemented in two ways, showcasing how pipeline complexity evolves as a platform matures.

### Approach 1 — Raw SQL ETL Jobs ([`sql-jobs/`](./sql-jobs))

Standalone Snowflake SQL scripts. This is the "V1" pattern: no abstraction layer,
no templating — just direct `MERGE INTO` logic you can run as scheduled SQL jobs or
embed in an orchestration tool.

| Script | What it does |
|--------|-------------|
| [`raw_data_migration.sql`](./sql-jobs/raw_data_migration.sql) | Consolidates GitHub data from multiple Fivetran connectors (multi-org) into unified RAW tables using `UNION BY NAME` + `QUALIFY ROW_NUMBER()` dedup |
| [`reporting_data_model.sql`](./sql-jobs/reporting_data_model.sql) | Builds `BRIDGE_GITHUB_LDAP` (multi-source identity resolution), `FACT_PULL_REQUESTS` (bot classification, HASHDIFF), and `FACT_PR_REVIEW_SUMMARY` (self-review detection) |
| [`lead_time_to_deploy.sql`](./sql-jobs/lead_time_to_deploy.sql) | 7-stage DORA lead time pipeline: service registry mapping → commit-file mapping → longest-prefix service assignment → PR timeline → SHA match (staging + prod) → time-based fallback → final MERGE |

**Patterns demonstrated:**
- Multi-connector `UNION BY NAME` with `QUALIFY` deduplication
- Incremental `MERGE INTO` with `HASHDIFF` change detection
- Multi-source GitHub → LDAP identity resolution
- Longest-prefix match for monorepo service attribution
- SHA-based deployment matching with time-based fallback
- DORA bucket classification (elite / high / medium / low)

---

### Approach 2 — dbt Project ([`dbt-github-analytics/`](./dbt-github-analytics))

The same logic refactored as a dbt project. Templating removes duplication,
tests enforce data contracts, and incremental models handle scale.

```
Staging                     Intermediate                  Marts
──────────────────────────────────────────────────────────────────
stg_github__pull_requests   int_github__pr_lifecycle      fct_github__pull_requests
stg_github__commits         int_github__lead_time         fct_github__lead_time_to_deploy
stg_github__reviews                                       fct_github__commit_files
stg_github__repositories                                  fct_github__pr_times
stg_identity__user_mapping                                dim_github__users (SCD2)
                                                          dim_github__repositories
                                                          bridge_github__pr_labels
```

**Improvements over raw SQL:**
- SCD Type 2 `dim_github__users` — correct team attribution at point in time
- `incremental_predicates` for efficient Snowflake micro-partition pruning
- Schema tests (`unique`, `not_null`, `dbt_utils.expression_is_true`) enforce grain
- `persist_docs` — column descriptions flow through to Snowflake information schema
- Jinja templating eliminates multi-org copy-paste

---

## Architecture

```
GitHub API (REST)
       │
Fivetran / Prefect ETL  ──►  Snowflake RAW Layer
                                      │
                         ─────────────────────────────────
                         Option A: sql-jobs/ (scheduled SQL)
                         Option B: dbt-github-analytics/
                         ─────────────────────────────────
                                      │
                         BI Dashboards (DORA · Engineering Velocity · Code Quality)
```

## Projects

### [`sql-jobs/`](./sql-jobs)
Three raw SQL scripts covering the full pipeline from multi-org data consolidation through DORA lead time calculation. Designed to be run as scheduled Snowflake Tasks or orchestrated via Airflow/Prefect.

### [`dbt-github-analytics/`](./dbt-github-analytics)
Full staging → intermediate → marts dbt project following best-practice layering.

### [`etl-pipeline/`](./etl-pipeline)
Prefect flow for GitHub REST API ingestion: paginated PR fetch → PyArrow serialisation → GCS staging → Snowflake COPY + MERGE.

---

## Interesting Engineering Challenges

- **Multi-org identity resolution**: GitHub usernames don't map 1:1 to internal LDAP. A multi-source pipeline (identity cache + org email extraction + manual mapping) resolves them with `QUALIFY ROW_NUMBER()` deduplication.
- **Lead time matching cascade**: Exact commit SHA match is preferred; time-based fallback (first deployment after merge) activates when SHA data is unavailable or squash merges change the SHA.
- **Monorepo service attribution**: Longest-prefix match against a service registry maps each changed file to the correct service — handles deeply nested monorepo structures.
- **Noisy commit detection**: Lock files, merge commits, and auto-generated files inflate commit metrics. Flagged in `FACT_COMMIT_FILES` and excluded from velocity calculations.
- **SCD Type 2 user dimension**: Engineers move teams. Historical PRs correctly attribute to the team the engineer was on at the time.

## Tech Stack

`Python 3.11` · `Prefect 3.x` · `dbt Core` · `Snowflake` · `PyArrow` · `GitHub REST API` · `Fivetran`
