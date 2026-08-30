with orders_daily as (

    select * from {{ ref('int_orders_daily') }}

),

bounds as (

    select
        min(order_date) as min_date,
        max(order_date) as max_date
    from orders_daily

),

date_spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast(current_date() as date)"
    ) }}

),

final as (

    select
        date_spine.date_day                        as ds,
        coalesce(orders_daily.order_count, 0)       as y

    from date_spine
    cross join bounds
    left join orders_daily
        on date_spine.date_day = orders_daily.order_date
    where date_spine.date_day between bounds.min_date and bounds.max_date

)

select * from final
order by ds