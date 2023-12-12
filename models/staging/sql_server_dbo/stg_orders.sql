{{
  config(
    materialized='incremental' ,
    unique_key = 'order_id'
  )
}}

with src_orders as (

    SELECT
        cast (order_id as VARCHAR (128)) as order_id,
        cast (user_id as VARCHAR (128)) as user_id,
        cast (address_id as VARCHAR (128)) as address_id,
        CASE
            WHEN status = 'preparing' THEN 'undefined'
            ELSE COALESCE(tracking_id, 'undefined')
        END AS tracking_id ,
        to_date (created_at) as created_date_utc ,
        to_time (created_at) as created_time_utc ,
        to_date(estimated_delivery_at) as estimated_delivery_date_utc ,
        to_time(estimated_delivery_at) as estimated_delivery_time_utc ,
        to_date(delivered_at) as delivered_date_utc ,
        to_time(delivered_at) as delivered_time_utc ,
        cast (status as varchar(50)) as status ,
        CASE
            WHEN status = 'preparing' THEN 'undefined'
            ELSE COALESCE(shipping_service, 'undefined')
        END AS shipping_service ,
        DECODE(
            promo_id,
            'task-force', 'task-force',
            'instruction set', 'instruction set',
            'leverage', 'leverage',
            'Optional', 'optional',
            'Mandatory', 'mandatory',
            'Digitized', 'digitized',
            '', 'sin promo'
          ) AS promo_id , 
        cast (shipping_cost as float) as shipping_cost_usd ,
        cast (order_cost as float) as order_cost_usd ,
        cast (order_total as float) as order_total_usd ,
        cast (_fivetran_synced as timestamp_ntz(9)) as date_load_utc

    FROM {{ source('sql_server_dbo', 'orders') }}
{% if is_incremental()%}
where _fivetran_synced > (select max (date_load_utc) from {{this}})
{%endif%}
),

stg_orders_casted as (
    SELECT
        order_id ,
        user_id ,
        address_id ,
        tracking_id ,
        created_time_utc,
        created_date_utc ,
        estimated_delivery_date_utc ,
        estimated_delivery_time_utc ,
        delivered_date_utc ,
        delivered_time_utc ,
        {{ dbt_utils.generate_surrogate_key(['status']) }} AS  status_id ,
        status , 
        shipping_service ,
        {{ dbt_utils.generate_surrogate_key(['shipping_service']) }} AS shipping_service_id ,
        {{ dbt_utils.generate_surrogate_key(['promo_id']) }} AS promo_id , 
        shipping_cost_usd ,
        order_cost_usd ,
        order_total_usd ,
        date_load_utc 
    FROM src_orders
)

SELECT 
*
FROM stg_orders_casted


