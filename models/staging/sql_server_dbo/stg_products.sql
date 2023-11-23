{{
  config(
    materialized='view'
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

select
product_id,
price_usd,
name,
inventory 
from stg_products
union all 
SELECT 
    {{dbt_utils.generate_surrogate_key(['0000'])}}  ,
    0 , 
    'no_product' ,
    0 
