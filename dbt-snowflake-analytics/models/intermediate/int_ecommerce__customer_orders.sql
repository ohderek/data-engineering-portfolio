{#
int_ecommerce__customer_orders

Purpose: Aggregate order-level signals per customer to produce customer
         behavioural attributes used in CLV and segmentation models.

Grain: One row per customer_id.

Upstream: stg_ecommerce__orders
Downstream: dim_marketing__customers, fct_marketing__customer_lifetime_value
#}

{{
    config(
        materialized = "view",
        schema       = "intermediate",
    )
}}

with

orders as (

    select * from {{ ref("stg_ecommerce__orders") }}

),

aggregated as (

    select
        -- -------- ids
        customer_id,

        -- -------- order counts
        count(*)                                                          as total_orders,
        count_if(is_completed)                                            as completed_orders,
        count_if(is_cancelled)                                            as cancelled_orders,
        count_if(is_refunded)                                             as refunded_orders,

        -- -------- revenue
        sum(case when is_completed then total_amount else 0 end)          as lifetime_revenue,
        avg(case when is_completed then total_amount end)                 as avg_order_value,

        -- -------- temporal
        min(order_date)                                                   as first_order_date,
        max(order_date)                                                   as last_order_date,
        datediff(
            'day', min(order_date), max(order_date)
        )                                                                 as customer_lifespan_days,

        -- -------- derived ratios
        div0(
            count_if(is_completed), count(*)
        )                                                                 as completion_rate,

        div0(
            count_if(is_refunded), count_if(is_completed)
        )                                                                 as refund_rate

    from orders
    group by customer_id

),

final as (

    select
        -- -------- ids
        customer_id,

        -- -------- behavioural attributes
        total_orders,
        completed_orders,
        cancelled_orders,
        refunded_orders,
        lifetime_revenue,
        avg_order_value,

        -- -------- dates
        first_order_date,
        last_order_date,
        customer_lifespan_days,
        datediff('day', last_order_date, current_date())                 as days_since_last_order,

        -- -------- rates
        completion_rate,
        refund_rate,

        -- -------- rfm-style classification
        case
            when total_orders >= 10 and lifetime_revenue >= 1000 then 'champion'
            when total_orders >= 5  and lifetime_revenue >= 500  then 'loyal'
            when total_orders >= 2                                then 'repeat'
            else 'one_time'
        end                                                               as order_tier,

        -- -------- churn flag (no order in 90+ days)
        (datediff('day', last_order_date, current_date()) >= 90)::boolean as is_churned

    from aggregated

)

select * from final
