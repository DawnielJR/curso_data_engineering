
{{
  config(
    materialized='view'
  )
}}

with src_order_items as (

    select * from {{ source('sql_server_dbo', 'order_items') }}

),

stg_order_items as (

    select
        {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }} AS order_item_id,
        cast (order_id as VARCHAR (128)) as order_id,
        cast (product_id as VARCHAR (128)) as product_id,
        cast (quantity as INTEGER) as quantity,
        cast (_fivetran_synced as timestamp_ntz(9)) as date_load_utc

    from src_order_items

)

select 
*
from stg_order_items
order by order_id
