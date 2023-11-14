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
        product_id,
        price,
        name,
        inventory,
        _fivetran_deleted,
        _fivetran_synced as date_load

    from src_products

)

select * from stg_products
