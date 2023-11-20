with src_orders as (

    select * from {{ source('sql_server_dbo', 'orders') }}

),

stg_orders as (

    select
        order_id::varchar(50) as order_id,
        shipping_service::varchar() as shipping_service,
        shipping_cost::float as shipping_cost_USD,
        address_id::varchar(50) as address_id,
        created_at::timestamp_tz as created_at_UTC,
        CASE WHEN promo_id = '' THEN 'sin promo' ELSE promo_id END as promo_type,
        estimated_delivery_at::timestamp_tz as estimated_delivery_at_UTC,
        order_cost::float as order_cost_USD,
        user_id::varchar(50) as user_id,
        order_total::float as order_total_USD,
        delivered_at::timestamp_tz as delivered_at_UTC,
        tracking_id::varchar(50) as tracking_id,
        status::varchar(50) as status,
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_ntz(9) as date_load_UTC

    from src_orders
   

)

select * from stg_orders
