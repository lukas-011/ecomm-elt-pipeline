-- Mart: daily revenue by product. One row per (calendar day, product) for every
-- product that has ever sold, so a day on which a product sold nothing is a
-- real 0 rather than a missing row.
--
-- That shape serves both consumers: Metabase can rank products by revenue over
-- any window (and draw gap-free per-product trend lines), and Prophet can be
-- pointed at a single product's series without the missing days it can't
-- handle — the same reason fct_revenue_daily builds on a date spine.

with line_items as (

    select * from {{ ref('int_order_line_items') }}

),

realized_line_items as (

    -- Same exclusion as int_orders_daily: voided and refunded orders are not
    -- realized revenue. int_order_line_items keeps them deliberately, so the
    -- filter belongs here.
    select *
    from line_items
    where financial_status not in ('voided', 'refunded')

),

daily_by_product as (

    select
        order_date,
        product_id,
        sum(quantity)            as units_sold,
        sum(gross_line_revenue)  as gross_revenue,
        sum(net_line_revenue)    as net_revenue

    from realized_line_items
    group by order_date, product_id

),

sold_products as (

    -- Every product that has ever appeared on an order, with its attributes as
    -- captured at order time.
    select
        product_id,
        max(product_title) as order_time_title,
        max(vendor)        as order_time_vendor

    from realized_line_items
    group by product_id

),

products as (

    -- Prefer the current catalog record for naming and grouping in Metabase,
    -- but fall back to the order-time values so a product since deleted from
    -- the catalog (no stg_products match) still shows a name instead of NULL.
    select
        sold_products.product_id,
        coalesce(stg_products.product_title, sold_products.order_time_title) as product_title,
        coalesce(stg_products.vendor, sold_products.order_time_vendor)       as vendor,
        stg_products.product_type

    from sold_products
    left join {{ ref('stg_products') }} as stg_products
        on sold_products.product_id = stg_products.product_id

),

bounds as (

    select
        min(order_date) as min_date,
        max(order_date) as max_date
    from daily_by_product

),

date_spine as (

    -- NOTE: this macro expands to a nested `with rawdata / all_periods /
    -- filtered` chain. Snowflake resolves those names against this model's
    -- own CTEs, so none of the CTEs above may reuse one of those three
    -- names or the spine silently resolves to the wrong relation.
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast(current_date() as date)"
    ) }}

),

final as (

    -- The spine crossed with the product list is the full day x product grid;
    -- the left join hangs actual sales off it and COALESCE fills the rest in
    -- with zeros.
    select
        date_spine.date_day                         as order_date,
        products.product_id,
        products.product_title,
        products.vendor,
        products.product_type,
        coalesce(daily_by_product.units_sold, 0)    as units_sold,
        coalesce(daily_by_product.gross_revenue, 0) as gross_revenue,
        coalesce(daily_by_product.net_revenue, 0)   as net_revenue

    from date_spine
    cross join bounds
    cross join products
    left join daily_by_product
        on date_spine.date_day = daily_by_product.order_date
        -- product_id is NULL for deleted/custom items, and plain `=` never
        -- matches NULL, so those sales would silently flatten to 0. EQUAL_NULL
        -- matches NULL-to-NULL and keeps their revenue attached.
        and equal_null(products.product_id, daily_by_product.product_id)
    where date_spine.date_day between bounds.min_date and bounds.max_date

)

select * from final
order by order_date, gross_revenue desc
