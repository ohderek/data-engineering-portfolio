{#
fct_finance__revenue

Purpose: Daily revenue fact table at order-item grain, used as the canonical
         source for finance reporting, margin analysis, and BI dashboards.

Grain: One row per (order_item_id, order_date).

Upstream: int_ecommerce__order_items_enriched, stg_ecommerce__orders
Downstream: BI dashboards, revenue reconciliation jobs
#}

{{
    config(
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
    )
}}

with

order_items as (

    select * from {{ ref("int_ecommerce__order_items_enriched") }}

    {% if is_incremental() %}
        where updated_at >= (
            select dateadd(
                day,
                -{{ var('data_interval_lookback_days') }},
                max(updated_at)
            )
            from {{ this }}
        )
    {% endif %}

),

orders as (

    select
        order_id,
        order_date,
        currency,
        ship_to_country,
        customer_id

    from {{ ref("stg_ecommerce__orders") }}

),

joined as (

    select
        -- -------- keys
        {{ dbt_utils.generate_surrogate_key(['oi.order_item_id', 'o.order_date']) }}
                                                         as revenue_pk,
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        o.customer_id,

        -- -------- product dimensions
        oi.product_sku,
        oi.product_name,
        oi.product_category,
        oi.product_subcategory,
        oi.brand,

        -- -------- order dimensions
        o.currency,
        o.ship_to_country,

        -- -------- line financials
        oi.quantity,
        oi.unit_price,
        oi.discount,
        oi.gross_line_amount,
        oi.net_line_amount,
        oi.total_cost,
        oi.unit_margin,
        oi.margin_pct,

        -- -------- flags
        oi.has_discount,
        oi.is_deeply_discounted,
        oi.is_missing_product_lookup,

        -- -------- dates
        o.order_date,

        -- -------- audit
        oi.updated_at,
        oi.data_synced_at

    from order_items oi
    left join orders o on oi.order_id = o.order_id

),

final as (

    select * from joined

)

select * from final
