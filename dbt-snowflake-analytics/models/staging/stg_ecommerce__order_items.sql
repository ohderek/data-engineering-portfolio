{#
stg_ecommerce__order_items

Purpose: Thin staging wrapper over raw order_items. Deduplicates on
         order_item_id and casts numeric fields.

Grain: One row per order_item_id.

Source: {{ source('ecommerce', 'order_items') }}
#}

with

source as (

    select * from {{ source("ecommerce", "order_items") }}

),

renamed as (

    select
        -- -------- ids
        order_item_id,
        order_id,
        product_id,

        -- -------- strings
        product_sku::varchar            as product_sku,

        -- -------- numerics
        quantity::int                   as quantity,
        unit_price::float               as unit_price,
        coalesce(discount, 0)::float    as discount,

        -- -------- timestamps
        updated_at::timestamp_ntz       as updated_at,
        _loaded_at::timestamp_ntz       as data_synced_at

    from source

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by order_item_id
            order by updated_at desc, data_synced_at desc
        ) as _row_num

    from renamed

),

final as (

    select
        -- -------- ids
        order_item_id,
        order_id,
        product_id,

        -- -------- attributes
        product_sku,
        quantity,
        unit_price,
        discount,

        -- -------- derived
        (unit_price * quantity)::float                                as gross_line_amount,
        (unit_price * quantity * (1 - discount))::float               as net_line_amount,
        (discount > 0)::boolean                                       as has_discount,

        -- -------- timestamps
        updated_at,
        data_synced_at

    from deduplicated
    where _row_num = 1

)

select * from final
