{#
dim_marketing__customers

Purpose: Customer dimension with behavioural attributes and RFM-style
         classification. Thin wrapper combining staging + intermediate.

Grain: One row per customer_id.
#}

{{
    config(
        materialized = "table",
        alias        = "dim_customers",
    )
}}

with

customers as (

    select * from {{ ref("stg_ecommerce__customers") }}

),

customer_orders as (

    select * from {{ ref("int_ecommerce__customer_orders") }}

),

final as (

    select
        -- -------- ids
        c.customer_id,

        -- -------- identity (PII-free)
        c.email_hash,
        c.first_name,
        c.last_name,
        c.country_code,

        -- -------- segments
        c.customer_segment,
        c.acquisition_channel,

        -- -------- behavioural attributes (from intermediate)
        co.total_orders,
        co.completed_orders,
        co.lifetime_revenue,
        co.avg_order_value,
        co.completion_rate,
        co.refund_rate,
        co.order_tier,
        co.is_churned,

        -- -------- temporal
        c.customer_since_date,
        co.first_order_date,
        co.last_order_date,
        co.customer_lifespan_days,
        co.days_since_last_order,

        -- -------- audit
        c.updated_at,
        c.data_synced_at

    from customers c
    left join customer_orders co on c.customer_id = co.customer_id

)

select * from final
