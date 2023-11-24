with src_orders as (

    select * from {{ source('sql_server_dbo', 'orders') }}

),

stg_orders as (

    select
        cast (order_id as STRING) as order_id,
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

select 
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS order_key ,
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_key,
    {{ dbt_utils.generate_surrogate_key(['created_at_utc']) }} AS created_date_key,
    created_at_utc,
    order_cost_usd,
    {{ dbt_utils.generate_surrogate_key(['status']) }} AS status_key,
    status,
    {{ dbt_utils.generate_surrogate_key(['shipping_service']) }} AS shipping_service_key,
    shipping_service,
    shipping_cost_usd,
    order_total_usd,
    {{ dbt_utils.generate_surrogate_key(['address_id']) }} AS address_key,
    {{ dbt_utils.generate_surrogate_key(['estimated_delivery_date_utc']) }} AS estimated_delivery_date_utc_key,
    estimated_delivery_date_utc,
    {{ dbt_utils.generate_surrogate_key(['estimated_delivery_time_utc']) }} AS estimated_delivery_time_utc_key,
    estimated_delivery_time_utc,
    {{ dbt_utils.generate_surrogate_key(['delivered_date_utc']) }} AS delivered_date_utc_key,
    delivered_date_utc,
    {{ dbt_utils.generate_surrogate_key(['delivered_time_utc']) }} AS delivered_time_utc_key,
    delivered_time_utc,
    tracking_id,
    {{ dbt_utils.generate_surrogate_key(['promo_id']) }} AS promo_key,
    date_load_utc

 from stg_orders
