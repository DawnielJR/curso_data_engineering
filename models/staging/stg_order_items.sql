
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
        order_id,
        product_id,
        quantity,
        _fivetran_synced as date_load

    from src_order_items

)

select * from stg_order_items
