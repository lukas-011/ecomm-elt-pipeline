-- Staging model for Shopify orders.
-- Light cleanup only (rename + type-cast): one row per order, materialized as a
-- view so it always reflects the latest RAW load. Downstream marts build on this
-- rather than touching the source table directly.

with source as (

    select * from {{ source('gym_whale', 'orders') }}

),

renamed as (

    select
        id                          as order_id,
        customer_id,
        order_number,

        -- Keep the full timestamp for detail, plus a date grain for daily rollups
        -- (the order_volume mart and Prophet work on a daily series).
        created_at                  as ordered_at,
        cast(created_at as date)    as order_date,

        total_price                 as order_total,
        financial_status,
        currency,
        line_items,
        updated_at,
        loaded_at

    from source

)

select * from renamed