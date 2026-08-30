with source as (

    select * from {{ source('shopify', 'products') }}

),

renamed as (

    select
        id                          as product_id,
        title                       as product_title,
        product_type,
        vendor,
        handle,
        cast(created_at as date)    as created_date,
        created_at,
        tags,
        loaded_at

    from source

)

select * from renamed