{{
  config(
     materialized='view'
    ,unique_key='address_id'
  )
}}
with
src_addresses as (select * from {{ source("sql_server_dbo", "addresses") }}),

stg_addresses as (

    select
        cast (address_id as VARCHAR (128)) as address_id,
        cast (zipcode as INTEGER) as zipcode,
        cast (country as VARCHAR (128)) as country,
        cast (address as VARCHAR (128)) as address,
        cast (state as VARCHAR (128)) as state,
        _fivetran_synced as date_load

    from src_addresses

)

select 
*
from stg_addresses
