with orders as (

    select *
    from {{ ref('stg_orders') }}

),

filtered as (

    select *
    from orders
    where financial_status not in ('voided', 'refunded')

),

daily_aggregated as (

    select
        order_date,
        count(order_id)        as order_count,
        sum(order_total_price)       as order_revenue

    from filtered
    group by order_date

)

select * from daily_aggregated