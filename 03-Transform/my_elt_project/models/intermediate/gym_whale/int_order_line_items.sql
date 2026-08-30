-- Intermediate model: explodes the raw Shopify `line_items` JSON array carried
-- on stg_orders into one row per (order, line item).
--
-- Shopify returns every item on an order as an element of a single VARIANT
-- column, so "which product earned the most revenue" cannot be answered from
-- stg_orders alone. LATERAL FLATTEN turns that array into rows: the order row
-- is repeated once per array element, and `line_item.value` holds that
-- element's JSON object, which is then picked apart with the `:` path operator.
--
-- This is a faithful 1:1 flattening — no orders are filtered out here, so each
-- consumer can decide how to treat voided/refunded orders. `financial_status`
-- is carried through for exactly that purpose (fct_revenue_by_product filters
-- on it the same way int_orders_daily does).

with orders as (

    select * from {{ ref('stg_orders') }}

),

flattened as (

    select
        orders.order_id,
        orders.order_date,
        orders.financial_status,

        -- `:key` reads a field out of the VARIANT element and `::type` casts it
        -- out of VARIANT into a real column type.
        line_item.value:id::number                              as line_item_id,
        line_item.value:product_id::number                      as product_id,
        line_item.value:variant_id::number                      as variant_id,
        line_item.value:sku::varchar                            as sku,
        line_item.value:title::varchar                          as product_title,
        line_item.value:vendor::varchar                         as vendor,
        line_item.value:quantity::number                        as quantity,

        -- Shopify sends money as JSON strings ("54.99"), so go through varchar
        -- before the numeric cast rather than relying on VARIANT coercion.
        line_item.value:price::varchar::numeric(10,2)           as unit_price,
        line_item.value:total_discount::varchar::numeric(10,2)  as line_discount

    from orders,
        lateral flatten(input => orders.line_items) as line_item

),

final as (

    select
        *,
        quantity * unit_price                                as gross_line_revenue,

        -- Shopify's total_discount is the discount for the whole line, not per
        -- unit, so it is subtracted once rather than multiplied by quantity.
        (quantity * unit_price) - coalesce(line_discount, 0) as net_line_revenue

    from flattened

)

select * from final
