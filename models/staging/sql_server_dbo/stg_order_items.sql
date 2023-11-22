
{{
  config(
    materialized='table'
  )
}}

with src_order_items as (

    select * from {{ source('sql_server_dbo', 'order_items') }}

),

stg_order_items as (

    select
        cast (order_id as STRING) as order_id,
        cast (product_id as STRING) as product_id,
        cast (quantity as INT) as quantity,
        cast (_fivetran_synced as timestamp_ntz(9)) as date_load_utc

    from src_order_items

)

select * from stg_order_items
