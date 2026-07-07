-- Staging model for Shopify inventory levels.
-- One row per (inventory_item, location) stock level, materialized as a view.

with source as (

    select * from {{ source('gym_whale', 'inventory') }}

),

renamed as (

    select
        inventory_item_id,
        variant_id,
        location_id,
        available                   as available_quantity,
        tracked                     as is_tracked,
        cast(created_at as date)    as created_date,
        created_at,
        loaded_at

    from source

)

select * from renamed