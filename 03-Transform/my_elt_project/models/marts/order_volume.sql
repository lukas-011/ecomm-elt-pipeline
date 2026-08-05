
with source as (
    select
        id,
        product_id,
        location_id,
        available,
        tracked,
        created_at,
        loaded_at
    from {{ source('shopify', 'orders') }}
)