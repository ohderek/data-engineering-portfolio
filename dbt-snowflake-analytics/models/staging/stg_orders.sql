{{
    config(
        materialized = 'incremental',
        unique_key   = 'order_id',
        on_schema_change = 'sync_all_columns',
        tags = ['staging', 'ecommerce']
    )
}}

/*
  Staging model for orders.

  Responsibilities:
    - Deduplicate raw records (keep the latest _loaded_at per order_id)
    - Cast and validate data types
    - Apply consistent NULL handling
    - Rename to canonical column names

  Grain: one row per order_id
*/

with

source as (

    select * from {{ source('ecommerce', 'orders') }}

    {% if is_incremental() %}
        -- Only process records updated since the last run, with a lookback
        -- buffer to catch any late-arriving updates
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

deduplicated as (

    select
        order_id,
        customer_id,
        lower(trim(status))              as status,
        total_amount,
        upper(trim(currency))            as currency,
        created_at,
        updated_at,
        coalesce(ship_to_country, 'UNKNOWN')  as ship_to_country,
        _loaded_at,
        _source,

        -- Dedup: retain the record most recently loaded per order_id
        row_number() over (
            partition by order_id
            order by updated_at desc, _loaded_at desc
        ) as _row_num

    from source

),

final as (

    select
        -- Keys
        order_id,
        customer_id,

        -- Attributes
        status,
        total_amount,
        currency,
        ship_to_country,

        -- Derived
        date_trunc('day', created_at)    as order_date,
        datediff(
            'hour',
            created_at,
            coalesce(updated_at, current_timestamp)
        )                                as hours_to_last_update,

        -- Flags
        (status = 'completed')::boolean  as is_completed,
        (status = 'cancelled')::boolean  as is_cancelled,
        (status = 'refunded')::boolean   as is_refunded,

        -- Audit
        created_at,
        updated_at,
        _loaded_at,
        _source

    from deduplicated
    where _row_num = 1

)

select * from final
