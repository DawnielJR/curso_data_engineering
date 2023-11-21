with src_orders as (

    select * from {{ source('sql_server_dbo', 'orders') }}

),

stg_orders as (

    select
        cast (order_id as varchar(50)) as order_id,
        cast (shipping_service as varchar(50)) as shipping_service,
        cast (shipping_cost as float) as shipping_cost_usd,
        cast (address_id as varchar(50)) as address_id,
        cast (created_at as timestamp_tz) as created_at_utc,
        to_date(estimated_delivery_at) as estimated_delivery_date_utc,
        to_time(estimated_delivery_at) as estimated_delivery_time_utc,
        cast (order_cost as float) as order_cost_usd,
        cast (user_id as varchar(50)) as user_id,
        cast (order_total as float) as order_total_usd,
        to_date(delivered_at) as delivered_date_utc,
        to_time(delivered_at) as delivered_time_utc,
        CASE 
            WHEN tracking_id = '' THEN 'preparing'
            ELSE tracking_id 
            END AS tracking_id,
        promo_id, --este campo lo comprobamos para la integridad visual con el HASH que viene de la tabla PROMOS
        cast({{dbt_utils.generate_surrogate_key(['promo_id'])}} as STRING) as promo_id,
        cast (status as varchar(50)) as status,
        cast (_fivetran_synced as timestamp_ntz(9)) as date_load_utc

    from src_orders
   

)

select * from stg_orders
