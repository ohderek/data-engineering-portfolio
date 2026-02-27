# dbt-snowflake-analytics

Production dbt project for the e-commerce analytics platform. Transforms raw Snowflake data through a layered architecture into finance and marketing marts consumed by BI dashboards and ML pipelines.

## Architecture

```
RAW Layer (Snowflake)
    │
    ▼
┌─────────────────────────────────────────────────────┐
│  Staging  (views)                                    │
│  stg_ecommerce__orders                              │
│  stg_ecommerce__order_items                         │
│  stg_ecommerce__customers                           │
│  stg_ecommerce__products                            │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│  Intermediate  (views / incremental)                 │
│  int_ecommerce__order_items_enriched                │
│  int_ecommerce__customer_orders                     │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│  Marts  (tables, persist_docs)                       │
│  finance/   fct_finance__revenue                    │
│  marketing/ dim_marketing__customers                │
└─────────────────────────────────────────────────────┘
```

## Project Structure

```
dbt-snowflake-analytics/
├── dbt_project.yml            # Layer materializations, schema config
├── profiles.yml               # Snowflake targets (local / production)
├── packages.yml               # dbt_utils, dbt_project_evaluator
├── models/
│   ├── staging/
│   │   ├── _sources.yml       # Source definitions & freshness checks
│   │   ├── _stg_ecommerce__models.yml
│   │   ├── stg_ecommerce__orders.sql
│   │   ├── stg_ecommerce__order_items.sql
│   │   ├── stg_ecommerce__customers.sql
│   │   └── stg_ecommerce__products.sql
│   ├── intermediate/
│   │   ├── int_ecommerce__order_items_enriched.sql
│   │   └── int_ecommerce__customer_orders.sql
│   └── marts/
│       ├── finance/
│       │   ├── _finance__models.yml
│       │   └── fct_finance__revenue.sql
│       └── marketing/
│           ├── _marketing__models.yml
│           └── dim_marketing__customers.sql
├── snapshots/
│   └── snp_ecommerce__products.sql   # SCD Type 2 product catalogue
├── macros/
│   ├── generate_schema_name.sql      # Environment-aware schema routing
│   └── utils.sql                     # hash_pii, normalize_order_status, safe_divide
└── tests/
    ├── assert_net_revenue_not_exceeds_gross.sql
    └── assert_orders_have_valid_customers.sql
```

## Naming Conventions

| Layer | Prefix | Example |
|-------|--------|---------|
| Staging | `stg_<source>__<entity>` | `stg_ecommerce__orders` |
| Intermediate | `int_<domain>__<concept>` | `int_ecommerce__order_items_enriched` |
| Fact | `fct_<domain>__<concept>` | `fct_finance__revenue` |
| Dimension | `dim_<domain>__<concept>` | `dim_marketing__customers` |
| Snapshot | `snp_<source>__<entity>` | `snp_ecommerce__products` |

## Development

```bash
# Install dependencies
pip install dbt-snowflake
dbt deps

# Local development
export USER=yourname
dbt run --target local --select staging

# Run with tests
dbt build --target local

# Run governance checks
dbt build --select package:dbt_project_evaluator \
  --vars '{"dbt_project_evaluator_enabled": true}'

# Check source freshness
dbt source freshness
```

## Testing Strategy

- **Schema tests**: `not_null`, `unique`, `accepted_values` defined in `_models.yml`
- **Combination tests**: `dbt_utils.unique_combination_of_columns` on all composite keys
- **Expression tests**: `dbt_utils.expression_is_true` for business rules (e.g. net ≤ gross)
- **Custom tests**: SQL files in `tests/` for referential integrity and domain logic
- **Severity**: All tests default to `warn` — escalated to `error` only on critical models
- **Governance**: `dbt_project_evaluator` runs in CI to enforce naming and DAG conventions

## Key Design Decisions

- **PII at boundary**: Email is SHA-256 hashed in `stg_ecommerce__customers`. Raw PII never reaches marts.
- **Idempotent incremental**: All incremental models use `merge` with a lookback buffer (`data_interval_lookback_days`) to catch late arrivals.
- **Dynamic warehouse**: Mart models allocate larger warehouses for full-refresh builds, smaller for incremental runs.
- **SCD Type 2 for products**: `snp_ecommerce__products` tracks cost_price and category history so historical margin is accurate.
