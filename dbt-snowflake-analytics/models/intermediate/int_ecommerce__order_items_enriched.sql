{#
int_ecommerce__order_items_enriched

Purpose: Enrich order line items with product attributes and compute
         line-level revenue and margin metrics.

Grain: One row per order_item_id.

Upstream: stg_ecommerce__order_items, stg_ecommerce__products
Downstream: fct_finance__revenue, fct_finance__order_summary
#}

{{
    config(
        materialized = "view",
        schema       = "intermediate",
    )
}}

with

order_items as (

    select * from {{ ref("stg_ecommerce__order_items") }}

),

products as (

    select * from {{ ref("stg_ecommerce__products") }}

),

enriched as (

    select
        -- -------- ids
        oi.order_item_id,
        oi.order_id,
        oi.product_id,

        -- -------- product attributes (snapshotted at order time)
        p.product_sku,
        p.product_name,
        p.product_category,
        p.product_subcategory,
        p.brand,
        p.cost_price,

        -- -------- line financials (from staging)
        oi.quantity,
        oi.unit_price,
        oi.discount,
        oi.gross_line_amount,
        oi.net_line_amount,

        -- -------- derived margin metrics
        round(p.cost_price * oi.quantity, 2)                                    as total_cost,

        round(
            (oi.unit_price * (1 - oi.discount)) - p.cost_price,
            2
        )                                                                        as unit_margin,

        round(
            div0(
                (oi.unit_price * (1 - oi.discount)) - p.cost_price,
                oi.unit_price * (1 - oi.discount)
            ),
            4
        )                                                                        as margin_pct,

        -- -------- flags
        oi.has_discount,
        (oi.discount >= 0.20)::boolean                                          as is_deeply_discounted,
        (p.product_id is null)::boolean                                          as is_missing_product_lookup,

        -- -------- timestamps
        oi.updated_at,
        oi.data_synced_at

    from order_items oi
    left join products p on oi.product_id = p.product_id

)

select * from enriched
