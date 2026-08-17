-- Staging model for Shopify orders.
-- Light cleanup only (rename + type-cast): one row per order, materialized as a
-- view so it always reflects the latest RAW load. Downstream marts build on this
-- rather than touching the source table directly.

with source as (

    select * from {{ source('shopify', 'orders') }}

),

renamed as (

    select
        id                          as order_id,
        customer_id,
        order_number,
        created_at,
        total_price                 as order_total_price,
        financial_status,
        currency,
        line_items,
        updated_at,
        loaded_at,
        cast(processed_at as date)    as order_date

    from source

)

select * from renamed