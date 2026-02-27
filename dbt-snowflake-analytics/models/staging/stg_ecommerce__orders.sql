{#
stg_ecommerce__orders

Purpose: Thin staging wrapper over raw orders. Renames columns to canonical
         snake_case, applies type casts, deduplicates on order_id keeping the
         latest updated_at, and filters soft-deleted rows.

Grain: One row per order_id.

Source: {{ source('ecommerce', 'orders') }}
#}

with

source as (

    select * from {{ source("ecommerce", "orders") }}

),

renamed as (

    select
        -- -------- ids
        order_id,
        customer_id,

        -- -------- strings
        lower(trim(status))           as status,
        upper(trim(currency))         as currency,
        coalesce(ship_to_country, 'UNKNOWN')::varchar as ship_to_country,

        -- -------- numerics
        total_amount::float           as total_amount,

        -- -------- timestamps
        created_at::timestamp_ntz     as created_at,
        updated_at::timestamp_ntz     as updated_at,
        _loaded_at::timestamp_ntz     as data_synced_at

    from source
    where not coalesce(_is_deleted, false)

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by order_id
            order by updated_at desc, data_synced_at desc
        ) as _row_num

    from renamed

),

final as (

    select
        -- -------- ids
        order_id,
        customer_id,

        -- -------- attributes
        status,
        currency,
        ship_to_country,
        total_amount,

        -- -------- derived flags
        (status = 'completed')::boolean as is_completed,
        (status = 'cancelled')::boolean as is_cancelled,
        (status = 'refunded')::boolean  as is_refunded,

        -- -------- dates
        created_at::date                as order_date,
        created_at,
        updated_at,
        data_synced_at

    from deduplicated
    where _row_num = 1

)

select * from final
