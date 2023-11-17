{{
  config(
    materialized='table'
  )
}}

with src_addresses as (

    select * from {{ source('sql_server_dbo', 'addresses') }}

),

stg_addresses as (

    select
        address_id,
        zipcode,
        country,
        address,
        state,
        _fivetran_synced as date_load

    from src_addresses

)

select * from stg_addresses
