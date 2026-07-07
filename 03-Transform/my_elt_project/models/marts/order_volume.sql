-- Daily order-volume mart: the input Prophet forecasts on (04-Forecast).
--
-- Prophet needs a regular, gap-free time series, where a day with no orders is a
-- real 0 (not a missing row). So we build a continuous daily date spine spanning
-- the order history and LEFT JOIN the actual counts onto it, coalescing absent
-- days to 0. The forecast script then renames order_date -> ds and order_count
-- -> y to feed Prophet directly.

with orders as (

    select * from {{ ref('stg_orders') }}

),

-- Real first/last order dates, used to trim the spine to the data's actual range.
bounds as (

    select
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from orders

),

-- dbt_utils builds one row per calendar day; we over-generate then bound below.
spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2016-01-01' as date)",
        end_date="cast(current_date as date)"
    ) }}

),

calendar as (

    select cast(spine.date_day as date) as order_date
    from spine
    cross join bounds
    where cast(spine.date_day as date)
        between bounds.first_order_date and bounds.last_order_date

),

-- Actual orders rolled up to the daily grain.
daily_orders as (

    select
        order_date,
        count(distinct order_id) as order_count,
        sum(order_total)         as gross_revenue
    from orders
    group by 1

),

final as (

    select
        calendar.order_date,
        coalesce(daily_orders.order_count, 0)   as order_count,
        coalesce(daily_orders.gross_revenue, 0) as gross_revenue
    from calendar
    left join daily_orders
        on calendar.order_date = daily_orders.order_date

)

select * from final
order by order_date