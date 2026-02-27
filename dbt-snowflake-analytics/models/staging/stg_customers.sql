{{
    config(
        materialized = 'incremental',
        unique_key   = 'customer_id',
        tags = ['staging', 'ecommerce']
    )
}}

/*
  Staging model for customers.

  Grain: one row per customer_id (latest record wins)
*/

with

source as (

    select * from {{ source('ecommerce', 'customers') }}

    {% if is_incremental() %}
        where updated_at >= (
            select dateadd(day, -{{ var('data_interval_lookback_days') }}, max(updated_at))
            from {{ this }}
        )
    {% endif %}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by updated_at desc, _loaded_at desc
        ) as _row_num

    from source

),

final as (

    select
        -- Keys
        customer_id,

        -- Identity (PII-free representation — emails are hashed downstream for analytics)
        {{ hash_pii('email') }}          as email_hash,
        lower(trim(first_name))          as first_name,
        lower(trim(last_name))           as last_name,
        upper(trim(country_code))        as country_code,

        -- Segments
        lower(trim(customer_segment))    as customer_segment,  -- vip | standard | wholesale
        lower(trim(acquisition_channel)) as acquisition_channel,

        -- Dates
        created_at::date                 as customer_since_date,
        updated_at,
        _loaded_at

    from deduplicated
    where _row_num = 1

)

select * from final
