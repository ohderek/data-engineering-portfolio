{% snapshot snp_ecommerce__products %}

    {{
        config(
            target_schema = "snapshots",
            unique_key    = "product_id",
            strategy      = "check",
            check_cols    = [
                "product_name",
                "product_category",
                "product_subcategory",
                "brand",
                "cost_price",
                "is_active",
            ],
        )
    }}

    /*
    Slowly changing dimension (Type 2) for the product catalogue.
    Tracks historical changes to product name, category, cost price,
    and active status so revenue models can attribute margin correctly
    for past periods.
    */

    select
        product_id,
        product_sku,
        product_name,
        product_category,
        product_subcategory,
        brand,
        cost_price,
        is_active,
        data_synced_at

    from {{ ref("stg_ecommerce__products") }}

{% endsnapshot %}
