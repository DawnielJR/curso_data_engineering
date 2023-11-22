{{
  config(
    materialized='table'
  )
}}


with src_products as (

    select * from {{ source('sql_server_dbo', 'products') }}

),

stg_products as (

    select
        cast (product_id as STRING) as product_id,
        cast (price as FLOAT) as price_usd,
        cast (name as VARCHAR (50)) as name,
        cast (inventory as INT) as inventory,
        cast (_fivetran_synced as timestamp_ntz(9)) as date_load_utc

    from src_products

)

select * from stg_products
