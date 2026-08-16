
with order_volume as (
    select
        order_date,
        count(order_id) as order_count,
        sum(order_total) as total_order_value
    from 
        {{ ref('stg_orders') }}
    where
        order_status in ('authorized', 'paid')
)