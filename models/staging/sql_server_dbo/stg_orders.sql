with src_orders as (

    select * from {{ source('sql_server_dbo', 'orders') }}

),

stg_orders as (

    select
        cast (order_id as varchar(50)) as order_id,
        cast (address_id as varchar(50)) as address_id,
        cast (user_id as varchar(50)) as user_id,
        decode(promo_id, '', 'sin_promo',null,'sin_promo',promo_id) as promo_id,
        decode(tracking_id, '', 'preparing', null, 'preparing', tracking_id) as tracking_id,
        cast (status as varchar(50)) as status,
        cast(decode (shipping_service,'', 'procesing',null,'procesing',shipping_service)as VARCHAR(50)) as shipping_service,
        cast (shipping_cost as float) as shipping_cost_usd,
        cast (order_cost as float) as order_cost_usd,
        cast (order_total as float) as order_total_usd,
        cast (created_at as timestamp_tz) as created_at_utc,
        to_date(estimated_delivery_at) as estimated_delivery_date_utc,
        to_time(estimated_delivery_at) as estimated_delivery_time_utc,
        to_date(delivered_at) as delivered_date_utc,
        to_time(delivered_at) as delivered_time_utc,
        cast (_fivetran_synced as timestamp_ntz(9)) as date_load_utc

    from src_orders
   

)

select * from stg_orders
