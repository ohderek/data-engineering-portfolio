# dbt-snowflake-analytics

> **Hypothetical Showcase:** A production-grade dbt project demonstrating best practices for analytics engineering on Snowflake. Covers project structure, naming conventions, materialization strategy, testing, documentation, snapshots, macros, and incremental modelling — grounded in the [dbt Labs best practices guide](https://docs.getdbt.com/best-practices) and dimensional modelling principles.

---

## What this project is

An e-commerce analytics platform that transforms raw transactional data through a three-layer architecture into finance and marketing marts. The data model implements Kimball-style dimensional modelling — fact tables at event grain joined to slowly-changing dimension tables — which is the dominant pattern for analytics warehouses because it optimises for BI query performance over storage efficiency.

The project is designed to be read as a reference, not just run. Every design decision is explained with the reasoning behind it.

---

## Data Modelling Philosophy

### Source-conformed → Business-conformed

The core principle behind this project's layered architecture comes directly from the [dbt best practices guide](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview):

> *"The essential principle underlying all dbt projects involves establishing a cohesive arc moving data from source-conformed to business-conformed."*

**Source-conformed** data reflects the external system's design — column names chosen by engineers, integer enums, inconsistent casing, OLTP normalisation. It is not shaped for human understanding or BI queries.

**Business-conformed** data embodies the organisation's concepts — `order_date` instead of `createdAt`, `is_completed` boolean instead of `status = 3`, revenue calculated the way Finance defines it. This is what analysts and BI tools consume.

The journey from one to the other is the job of the transformation layer.

### Dimensional Modelling (Kimball)

The mart layer follows the **Kimball dimensional model**:

- **Fact tables** (`fct_`) record business events at a defined grain (e.g. one row per order line item per day). They hold measures (revenue, quantity) and foreign keys to dimensions.
- **Dimension tables** (`dim_`) provide descriptive context about the entities in facts (customers, products). Deliberately denormalised — a BI query should never need to join three tables to find a customer's country.
- **Bridge tables** (`bridge_`) resolve many-to-many relationships (e.g. one incident affecting multiple services).

This produces a **star schema** — facts at the centre, dimensions radiating outward. The denormalisation trades storage efficiency for query speed and simplicity, which is the right tradeoff for analytical workloads.

```
                        dim_marketing__customers
                              │
                              │ customer_id
                              │
fct_finance__revenue ─────────┤ order_id
  grain: order_item × date    │
                              │ product_id ─── snp_ecommerce__products (SCD Type 2)
                              │
                        (product attrs denormalised into fct)
```

---

## Project Structure

```
dbt-snowflake-analytics/
│
├── dbt_project.yml            Project config: layer materializations, schemas, vars
├── packages.yml               dbt_utils, dbt_project_evaluator
├── profiles.yml               Snowflake connection targets (dev / prod)
│
├── models/
│   ├── staging/               Layer 1 — source-conformed atomic building blocks
│   │   ├── _sources.yml         Source definitions + freshness checks + source tests
│   │   ├── _stg_ecommerce__models.yml  Model docs + column-level tests
│   │   ├── stg_ecommerce__orders.sql
│   │   ├── stg_ecommerce__order_items.sql
│   │   ├── stg_ecommerce__customers.sql
│   │   └── stg_ecommerce__products.sql
│   │
│   ├── intermediate/          Layer 2 — business logic, aggregations, joins
│   │   ├── int_ecommerce__customer_orders.sql      Customer-grain order aggregations
│   │   └── int_ecommerce__order_items_enriched.sql Line-item enrichment with product data
│   │
│   └── marts/                 Layer 3 — business-conformed, analyst-facing
│       ├── finance/
│       │   ├── _finance__models.yml
│       │   └── fct_finance__revenue.sql            Incremental revenue fact
│       ├── marketing/
│       │   └── dim_marketing__customers.sql        Customer dimension (denormalised)
│       └── ops/
│           ├── fct_ops__incidents.sql
│           └── bridge_ops__incident_service.sql    Many-to-many: incident ↔ service
│
├── snapshots/
│   └── snp_ecommerce__products.sql   SCD Type 2 for the product catalogue
│
├── macros/
│   ├── generate_schema_name.sql      Environment-aware schema routing
│   └── utils.sql                     hash_pii(), safe_divide()
│
└── tests/
    ├── assert_net_revenue_not_exceeds_gross.sql
    └── assert_orders_have_valid_customers.sql
```

---

## Layer 1 — Staging

### Purpose

Staging models are thin wrappers over raw source tables. Their **single responsibility**: make the data safe and consistent to build on. Nothing more.

From the [dbt best practices guide](https://docs.getdbt.com/best-practices/how-we-structure/2-staging):

> *"Staging models create atomic building blocks — the foundational modular units that other layers build upon."*

A staging model should do exactly these things — and nothing else:
1. Rename columns to `snake_case`
2. Cast types to canonical forms (`TIMESTAMP_NTZ`, `FLOAT`, `BOOLEAN`)
3. Apply light cleaning: `lower(trim())`, `coalesce()` for nulls, `upper()` for codes
4. Deduplicate on the natural key
5. Filter soft-deleted rows (`where not _is_deleted`)
6. Hash PII at the boundary — raw PII **never** propagates downstream

It should **not** join other models, aggregate, or apply business logic. That belongs in intermediate.

### The Four-CTE Pattern

Every model follows the same structure, which is the dbt standard:

```sql
-- stg_ecommerce__orders.sql
with

source as (
    -- Never reference a table directly. Always use source() or ref().
    select * from {{ source("ecommerce", "orders") }}
),

renamed as (
    select
        -- ── ids ──────────────────────────────────────────────
        order_id,
        customer_id,

        -- ── strings ──────────────────────────────────────────
        lower(trim(status))                     as status,
        upper(trim(currency))                   as currency,
        coalesce(ship_to_country, 'UNKNOWN')    as ship_to_country,

        -- ── numerics ─────────────────────────────────────────
        total_amount::float                     as total_amount,

        -- ── timestamps ───────────────────────────────────────
        created_at::timestamp_ntz               as created_at,
        updated_at::timestamp_ntz               as updated_at,
        _loaded_at::timestamp_ntz               as data_synced_at

    from source
    where not coalesce(_is_deleted, false)
),

deduplicated as (
    select *,
        row_number() over (
            partition by order_id
            order by updated_at desc, data_synced_at desc
        ) as _row_num
    from renamed
),

final as (
    select
        order_id,
        customer_id,
        status,
        currency,
        ship_to_country,
        total_amount,
        -- Derived boolean flags at staging — avoids repeated CASE WHEN downstream
        (status = 'completed')::boolean         as is_completed,
        (status = 'cancelled')::boolean         as is_cancelled,
        (status = 'refunded')::boolean          as is_refunded,
        created_at::date                        as order_date,
        created_at,
        updated_at,
        data_synced_at
    from deduplicated
    where _row_num = 1
)

select * from final
```

The terminal `select * from final` is deliberate — you can insert a CTE anywhere in the chain without editing the bottom of the file.

### PII Hashing at the Boundary

Raw PII is hashed at staging using a custom macro. It **never** propagates to intermediate or marts:

```sql
-- stg_ecommerce__customers.sql
{{ hash_pii('email') }} as email_hash,

-- macros/utils.sql
{% macro hash_pii(column_name) %}
    sha2(lower(trim({{ column_name }})), 256)
{% endmacro %}
```

Centralising the hashing algorithm in a macro means: one place to update if you ever need to change the algorithm or add a salt.

### Naming Convention

`stg_<source>__<entity>` — the double underscore separates the source system from the entity:

```
stg_ecommerce__orders             ← orders from the ecommerce source
stg_ecommerce__customers          ← customers from the ecommerce source
stg_incident_io__companya__incidents  ← incidents from incident.io (company A workspace)
```

In a project with 5+ source systems, `stg_stripe__charges` vs `stg_ecommerce__charges` are unambiguous at a glance.

### Source Definitions (`_sources.yml`)

Every source table is declared once in `_sources.yml`. Models reference it via `{{ source() }}` — never by raw table name:

```yaml
sources:
  - name: ecommerce
    database: "{{ var('ecommerce_database') }}"  # configurable per environment
    schema:   "{{ var('ecommerce_schema') }}"
    freshness:
      warn_after:  { count: 2,  period: hour }
      error_after: { count: 12, period: hour }
    loaded_at_field: _loaded_at                  # `dbt source freshness` checks this

    tables:
      - name: orders
        columns:
          - name: order_id
            tests: [not_null]
          - name: status
            tests:
              - accepted_values:
                  values: [pending, processing, completed, cancelled, refunded]
```

`dbt source freshness` queries `MAX(_loaded_at)` per table and alerts before downstream models run on stale data — a data quality gate before transformation begins.

### Materialization: View

All staging models are views:

```yaml
# dbt_project.yml
staging:
  +materialized: view
  +schema: staging
```

Staging is queried only during dbt runs, not by analysts or BI tools. Views cost nothing to build and always reflect current raw data. There is no reason to materialise staging as a table.

---

## Layer 2 — Intermediate

### Purpose

Intermediate models apply **business logic** that is too complex for staging but not yet a final mart entity. They prepare staging models for the joins and aggregations that produce marts.

From the [dbt best practices guide](https://docs.getdbt.com/best-practices/how-we-structure/3-intermediate):

> *"Intermediate models stack logical transformation steps with specific purposes, preparing staging models for downstream joins."*

The key rule: **only `ref()` other models here, never `source()`**. If a model references a source directly, it belongs in staging.

### Example: Customer Behavioural Aggregation

`int_ecommerce__customer_orders.sql` aggregates orders per customer to produce behavioural attributes used by `dim_marketing__customers`:

```sql
aggregated as (
    select
        customer_id,
        count(*)                                            as total_orders,
        count_if(is_completed)                              as completed_orders,
        sum(case when is_completed then total_amount end)   as lifetime_revenue,
        avg(case when is_completed then total_amount end)   as avg_order_value,
        min(order_date)                                     as first_order_date,
        max(order_date)                                     as last_order_date,
        datediff('day', min(order_date), max(order_date))   as customer_lifespan_days,

        -- Safe division: returns null instead of error on zero denominator
        div0(count_if(is_completed), count(*))              as completion_rate,
        div0(count_if(is_refunded), count_if(is_completed)) as refund_rate,

        -- RFM-style tier classification (Recency / Frequency / Monetary)
        case
            when total_orders >= 10 and lifetime_revenue >= 1000 then 'champion'
            when total_orders >= 5  and lifetime_revenue >= 500  then 'loyal'
            when total_orders >= 2                                then 'repeat'
            else 'one_time'
        end as order_tier,

        -- Churn flag: no order in 90+ days
        (datediff('day', last_order_date, current_date()) >= 90)::boolean as is_churned

    from orders
    group by customer_id
)
```

`div0()` is Snowflake's built-in safe division — returns `null` instead of crashing on zero. Use it everywhere a denominator can be zero.

### Naming Convention

`int_<domain>__<transformation_description>` — the name describes what the model *does*:

```
int_ecommerce__customer_orders          ← customer-grain aggregation of orders
int_ecommerce__order_items_enriched     ← line items joined to product attributes
int_ops__incidents_unioned              ← incidents UNION ALL-ed from 3 workspaces
```

### Materialization: View

Intermediate models are views. They are only queried during mart builds, never interactively. The [dbt guide](https://docs.getdbt.com/best-practices/materializations/1-guide-overview) recommends starting with views and only upgrading when query performance becomes a problem for end users.

---

## Layer 3 — Marts

### Purpose

Marts are the business-conformed, analyst-facing output of the entire project. They are the **only layer** that analysts, BI tools, and ML pipelines should query.

Marts are organised by **business domain** (finance, marketing, ops) — not by source system. This reflects that marts represent business concepts, not source system design.

### Fact Table: `fct_finance__revenue`

The revenue fact table sits at **order item × date grain** — the finest available. Coarser aggregations (daily, monthly revenue) are computed on top of this. You can never disaggregate once you've lost grain.

```sql
{{ config(
    materialized          = "incremental",
    incremental_strategy  = "merge",
    unique_key            = ["order_item_id", "order_date"],
    cluster_by            = ["order_date"],
    on_schema_change      = "append_new_columns",
    incremental_predicates = [
        "DBT_INTERNAL_DEST.order_date >= dateadd(day, -{{ var('data_interval_lookback_days') }}, current_date())"
    ],
    snowflake_warehouse = (
        ("ETL__LARGE" if is_incremental() else "ETL__XLARGE")
        if target.name == "production"
        else "ADHOC__" ~ env_var("WAREHOUSE", "MEDIUM")
    ),
    persist_docs = { "relation": true, "columns": true },
) }}
```

**Why composite `unique_key`?** `order_item_id` alone is the natural key, but declaring `(order_item_id, order_date)` enables Snowflake's micro-partition pruning during MERGE — it only scans partitions matching the `order_date` filter rather than the full table.

**Why `incremental_predicates`?** Without it, MERGE scans the entire target table for matching rows. `incremental_predicates` adds a `WHERE` clause to the *target side* of the MERGE, limiting the scan to recent partitions. On a billion-row table, this reduces scan cost by 99%+.

**Why `on_schema_change = "append_new_columns"`?** The default (`fail`) breaks the pipeline every time a new column is added, requiring `--full-refresh`. `append_new_columns` adds new columns to the existing table automatically — a much safer default for production marts that receive new columns regularly.

**Why different warehouses per run type?**
```python
("ETL__LARGE" if is_incremental() else "ETL__XLARGE")
```
Incremental runs process only new rows (small, fast). Full refreshes rebuild the entire table (large, slow). Matching warehouse size to workload saves significant compute cost.

### Surrogate Keys

```sql
{{ dbt_utils.generate_surrogate_key(['oi.order_item_id', 'o.order_date']) }} as revenue_pk
```

`dbt_utils.generate_surrogate_key` creates an MD5 hash of the natural key columns. Preferred over natural keys in marts because:
- Stable across source system migrations
- Database-agnostic — no Snowflake auto-increment dependency
- Deterministic — replaying the same data produces the same key

### Dimension Table: `dim_marketing__customers`

Built by joining staging customer attributes to intermediate behavioural signals:

```sql
-- dim_marketing__customers.sql  (conceptual)
select
    c.customer_id,
    c.email_hash,              -- PII-safe (hashed at staging)
    c.first_name,
    c.country_code,
    c.customer_segment,        -- CRM-defined segment
    c.acquisition_channel,     -- first-touch channel
    c.customer_since_date,
    -- Behavioural attributes from int_ecommerce__customer_orders
    o.total_orders,
    o.lifetime_revenue,
    o.avg_order_value,
    o.first_order_date,
    o.last_order_date,
    o.days_since_last_order,
    o.order_tier,              -- champion / loyal / repeat / one_time
    o.is_churned,
    o.completion_rate,
    o.refund_rate
from {{ ref('stg_ecommerce__customers') }}   c
left join {{ ref('int_ecommerce__customer_orders') }} o using (customer_id)
```

This is the **denormalised dimension** pattern — all customer context in one table. An analyst querying revenue by segment never joins anything. BI tool performance depends on this.

### Naming Convention

```
fct_<domain>__<noun>    ← fact table (events with measures)
dim_<domain>__<noun>    ← dimension table (entity attributes)
bridge_<domain>__<noun> ← bridge table (many-to-many)
```

### `persist_docs`

```yaml
# dbt_project.yml
marts:
  +persist_docs:
    relation: true
    columns: true
```

This pushes YAML descriptions to Snowflake as object comments. When an analyst hovers over a column in Snowflake's UI or in Looker/Tableau, they see the dbt description — no need to open the code. BI tools that read Snowflake metadata inherit these descriptions automatically.

---

## Materialization Decision Tree

The [dbt materialization best practices guide](https://docs.getdbt.com/best-practices/materializations/1-guide-overview) defines a simple escalation path:

```
Start with view
    ▼ (too slow for end users or build process)
Upgrade to table
    ▼ (full rebuilds take too long as data grows)
Implement incremental
```

Applied in this project:

| Layer | Materialization | Reasoning |
|---|---|---|
| Staging | `view` | Only queried during dbt runs, not by end users |
| Intermediate | `view` | Only consumed by mart builds, not interactive |
| Most marts | `table` | Queried by BI tools — must be fast |
| `fct_finance__revenue` | `incremental` | Billions of rows; full rebuild > 1 hour |

### Incremental model lookback window

```sql
{% if is_incremental() %}
    where updated_at >= (
        select dateadd(day, -{{ var('data_interval_lookback_days') }}, max(updated_at))
        from {{ this }}
    )
{% endif %}
```

The lookback window (`3 days` by default) accounts for late-arriving source data. Without a buffer, records with `updated_at` timestamps in the past would be missed on incremental runs. The window size should match your source system's maximum observed latency.

---

## Testing Strategy

dbt tests run at two levels.

### 1. Generic Tests (YAML)

Declared alongside column descriptions. Four built-in: `not_null`, `unique`, `accepted_values`, `relationships`. Extended by `dbt_utils`:

```yaml
# _stg_ecommerce__models.yml
- name: stg_ecommerce__orders
  data_tests:
    - dbt_utils.unique_combination_of_columns:
        combination_of_columns: [order_id]
  columns:
    - name: order_id
      tests: [not_null, unique]
    - name: status
      tests:
        - accepted_values:
            values: [pending, processing, completed, cancelled, refunded]

# _finance__models.yml
- name: fct_finance__revenue
  data_tests:
    - dbt_utils.expression_is_true:
        expression: "net_line_amount <= gross_line_amount"
        where: "order_date >= dateadd('day', -30, current_date())"
    - dbt_utils.expression_is_true:
        expression: "margin_pct is null or (margin_pct >= -1 and margin_pct <= 1)"
```

`dbt_utils.expression_is_true` is the most powerful test — any SQL boolean expression becomes a test. The `where` clause scopes it to recent data to avoid full-table scans in production.

### 2. Singular Tests (SQL files)

For business rules too complex for YAML. Any query that returns rows is a failure:

```sql
-- tests/assert_net_revenue_not_exceeds_gross.sql
-- Catches discount > 1.0 applied in the source system
select
    order_item_id,
    order_date,
    gross_line_amount,
    net_line_amount,
    discount
from {{ ref("fct_finance__revenue") }}
where
    order_date >= dateadd('day', -30, current_date())
    and net_line_amount > gross_line_amount
```

### Where to test

| Layer | What to test |
|---|---|
| Sources | `not_null` on PKs, `accepted_values` on enums — catch bad raw data before transformation |
| Staging | `unique` + `not_null` on grain column, FK `not_null`, status `accepted_values` |
| Intermediate | Grain uniqueness (compound key if needed) |
| Marts | All of the above + `expression_is_true` business rules, `unique` on surrogate key |

### Test severity

```yaml
# dbt_project.yml
data_tests:
  +severity: warn           # warn by default — bad rows don't block the pipeline
  dbt_project_evaluator:
    +severity: error        # governance violations fail the CI build
```

Setting most tests to `warn` prevents a single bad source record from blocking all downstream models. Individual tests on surrogate keys or critical business constraints should be overridden to `error`.

---

## Documentation

### Column descriptions in YAML

Every mart has a `_<domain>__models.yml` with full column descriptions:

```yaml
- name: margin_pct
  description: "(Net price - cost) / net price. NULL when cost data is unavailable."
  tests: [not_null]
```

With `persist_docs`, these are pushed to Snowflake object comments and visible in any tool that reads Snowflake metadata.

### Model-level grain documentation

Every SQL file begins with a Jinja comment declaring grain, upstream, and downstream:

```sql
{#
fct_finance__revenue

Purpose: Daily revenue fact at order-item grain.
Grain:   One row per (order_item_id, order_date).
Upstream: int_ecommerce__order_items_enriched, stg_ecommerce__orders
Downstream: BI dashboards, revenue reconciliation jobs
#}
```

**Documenting grain is the single most important documentation practice.** When two analysts disagree on a revenue number, the first question is always "what grain are you querying at?".

---

## Snapshots — SCD Type 2

Slowly Changing Dimensions (SCD Type 2) track the full change history of an entity. Without it, if a product's cost changed in March, you cannot correctly calculate margin for February — the current cost would be applied retroactively.

```sql
-- snapshots/snp_ecommerce__products.sql
{% snapshot snp_ecommerce__products %}

    {{ config(
        target_schema = "snapshots",
        unique_key    = "product_id",
        strategy      = "check",
        check_cols    = ["product_name", "product_category", "cost_price", "is_active"],
    ) }}

    select * from {{ ref("stg_ecommerce__products") }}

{% endsnapshot %}
```

`strategy = "check"` compares the listed columns on every run. When any change is detected:
1. The old row gets `dbt_valid_to = current_timestamp()`
2. A new row is inserted with `dbt_valid_from = current_timestamp()` and `dbt_valid_to = null`
3. The current record always has `dbt_valid_to IS NULL`

**Why snapshot the staging model, not the source?** Staging has already cleaned the data — proper types, deduplication. Snapshotting the raw source would capture noise changes (whitespace, type cast differences) as meaningful SCD events.

**SCD type comparison:**

| Type | Behaviour | Use when |
|---|---|---|
| Type 1 — Overwrite | Old value is lost, new value replaces it | Correcting data quality errors |
| Type 2 — History | Full history via date-effective rows | Business attributes affecting historical analysis (cost, team, price) |
| Type 3 — Add column | Current + previous value only | Very rare; limited history requirements |

---

## Macros

### `generate_schema_name` — Environment routing

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}                                -- dev: personal schema

    {%- elif "sandbox_" in default_schema -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}  -- sandbox: scoped to developer

    {%- else -%}
        {{ custom_schema_name | trim }}                     -- prod / ci: clean schema name

    {%- endif -%}

{%- endmacro %}
```

**Why this matters:**
- In **production** (`target.schema = ANALYTICS`): `fct_finance__revenue` lands in `FINANCE`
- In **developer sandbox** (`target.schema = sandbox_derek`): same model lands in `sandbox_derek_finance` — isolated, cannot overwrite production
- In **CI** (`target.schema = ci_pr_123`): lands in `FINANCE`, mirroring production exactly

Without this override, dbt's default produces `ANALYTICS_FINANCE` in production — polluting schema names everywhere.

### `hash_pii`

```sql
{% macro hash_pii(column_name) %}
    sha2(lower(trim({{ column_name }})), 256)
{% endmacro %}
```

One place to update the hashing algorithm, applied consistently across all staging models.

---

## Packages

### `dbt_utils`

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

| Macro / test | Used in | Purpose |
|---|---|---|
| `generate_surrogate_key` | `fct_finance__revenue` | Hash-based PK from composite key |
| `unique_combination_of_columns` | All staging + mart models | Compound grain uniqueness test |
| `expression_is_true` | `_finance__models.yml` | Arbitrary SQL boolean as a test |
| `accepted_range` | `_stg_ecommerce__models.yml` | Numeric range validation (quantity ≥ 1) |

### `dbt_project_evaluator`

Runs DAG validation rules as dbt tests. Enabled only in CI:

```yaml
dbt_project_evaluator:
  +enabled: "{{ var('dbt_project_evaluator_enabled', false) }}"

data_tests:
  dbt_project_evaluator:
    +severity: error   # governance violations fail the build
```

Rules it enforces:
- Every source must have a staging model
- Staging models must only reference `source()`, not `ref()` other staging models
- Mart models must not reference `source()` directly (must go through staging)
- Every model must have at least a one-line description
- Models should not have too many or too few upstream dependencies

---

## How to Implement

### Prerequisites

- Python 3.9+, `dbt-snowflake` (`pip install dbt-snowflake`)
- Snowflake account with a warehouse, database, and role
- Snowflake user with key-pair auth configured

### 1. Configure connection

```yaml
# profiles.yml
analytics:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your-account.snowflakecomputing.com
      user: your_username
      private_key_path: ~/.ssh/snowflake_key.p8
      role: TRANSFORMER
      warehouse: ADHOC__MEDIUM
      database: ANALYTICS
      schema: sandbox_yourname     # personal dev schema — isolated from production
      threads: 4

    prod:
      type: snowflake
      account: your-account.snowflakecomputing.com
      user: svc_dbt
      private_key_path: /secrets/snowflake_key.p8
      role: TRANSFORMER
      warehouse: ETL__LARGE
      database: ANALYTICS
      schema: ANALYTICS
      threads: 8
```

### 2. Install packages and run

```bash
dbt deps                             # install dbt_utils, dbt_project_evaluator

dbt source freshness                 # check raw data isn't stale before running

dbt run                              # run all models in dependency order
dbt test                             # run all tests

# Or as a single command:
dbt build                            # run + test, respecting dependency order

# Targeted runs
dbt run --select +fct_finance__revenue   # model + all upstream dependencies
dbt run --select fct_finance__revenue+   # model + all downstream dependants
dbt run --select tag:daily               # all models tagged 'daily'

# First-time full refresh of an incremental model
dbt run --full-refresh --select fct_finance__revenue

dbt snapshot                         # run SCD2 snapshots (separate from models)

dbt docs generate && dbt docs serve  # browse interactive DAG + data dictionary
```

### 3. CI/CD (GitHub Actions)

```yaml
# .github/workflows/dbt_ci.yml
on:
  pull_request:
    paths: ['dbt-snowflake-analytics/**']

jobs:
  dbt-ci:
    steps:
      - name: dbt deps
        run: dbt deps

      - name: dbt source freshness
        run: dbt source freshness

      - name: dbt build with governance checks
        # --defer --state: models unchanged from production are deferred,
        # not rebuilt. Only changed models + their downstream dependants run.
        # This is the dbt Labs recommended CI pattern.
        run: |
          dbt build \
            --target ci \
            --vars '{"dbt_project_evaluator_enabled": true}' \
            --defer \
            --state ./prod-artifacts
```

`--defer --state` is the [dbt Labs recommended CI pattern](https://docs.getdbt.com/best-practices): models unchanged from production are deferred rather than rebuilt, so CI only runs what actually changed in the PR plus its downstream dependants. This keeps CI fast and focused.

---

## Tech Stack

`dbt Core` · `Snowflake` · `dbt_utils` · `dbt_project_evaluator` · `Python`
