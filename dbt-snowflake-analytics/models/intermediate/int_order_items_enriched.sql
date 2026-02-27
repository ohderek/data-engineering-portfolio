{{
    config(
        materialized = 'ephemeral',
        tags = ['intermediate', 'ecommerce']
    )
}}

/*
  Intermediate model: enriches order_items with product info
  and calculates line-level revenue metrics.

  Grain: one row per order_item_id
  Upstream: stg_order_items, stg_products
  Downstream: fct_revenue, fct_order_summary
*/

with

order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

-- Calculate line-level financials
enriched as (

    select
        -- Keys
        oi.order_item_id,
        oi.order_id,
        oi.product_id,

        -- Product attributes (snapshot at time of order)
        p.product_name,
        p.product_category,
        p.product_subcategory,
        p.brand,
        p.cost_price,

        -- Line financials
        oi.quantity,
        oi.unit_price,
        oi.discount,

        -- Derived revenue metrics
        round(oi.unit_price * oi.quantity, 2)                                          as gross_revenue,
        round(oi.unit_price * oi.quantity * (1 - coalesce(oi.discount, 0)), 2)         as net_revenue,
        round(p.cost_price * oi.quantity, 2)                                           as total_cost,
        round(
            (oi.unit_price * (1 - coalesce(oi.discount, 0))) - p.cost_price,
            2
        )                                                                               as unit_margin,
        round(
            ((oi.unit_price * (1 - coalesce(oi.discount, 0))) - p.cost_price)
            / nullif(oi.unit_price * (1 - coalesce(oi.discount, 0)), 0),
            4
        )                                                                               as margin_pct,

        -- Flags
        (oi.discount > 0)::boolean          as has_discount,
        (oi.discount >= 0.20)::boolean      as is_deeply_discounted,

        -- Audit
        oi.updated_at

    from order_items oi
    left join products p on oi.product_id = p.product_id

)

select * from enriched
