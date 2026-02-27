{#
stg_ecommerce__customers

Purpose: Thin staging wrapper over the customers table. PII fields (email)
         are hashed at this layer — raw email never surfaces in analytics.

Grain: One row per customer_id (latest record per customer wins).

Source: {{ source('ecommerce', 'customers') }}
#}

with

source as (

    select * from {{ source("ecommerce", "customers") }}

),

renamed as (

    select
        -- -------- ids
        customer_id,

        -- -------- PII-safe attributes
        {{ hash_pii('email') }}              as email_hash,
        lower(trim(first_name))              as first_name,
        lower(trim(last_name))               as last_name,
        upper(trim(country_code))            as country_code,

        -- -------- segments
        lower(trim(customer_segment))        as customer_segment,
        lower(trim(acquisition_channel))     as acquisition_channel,

        -- -------- timestamps
        created_at::timestamp_ntz            as created_at,
        updated_at::timestamp_ntz            as updated_at,
        _loaded_at::timestamp_ntz            as data_synced_at

    from source
    where not coalesce(_is_deleted, false)

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by updated_at desc, data_synced_at desc
        ) as _row_num

    from renamed

),

final as (

    select
        -- -------- ids
        customer_id,

        -- -------- attributes
        email_hash,
        first_name,
        last_name,
        country_code,
        customer_segment,
        acquisition_channel,

        -- -------- dates
        created_at::date  as customer_since_date,

        -- -------- timestamps
        created_at,
        updated_at,
        data_synced_at

    from deduplicated
    where _row_num = 1

)

select * from final
