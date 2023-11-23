{{ config(materialized="view") }}

with
src_addresses as (select * from {{ source("sql_server_dbo", "addresses") }}),

stg_addresses as (

    select
        cast (address_id as VARCHAR (256)) as address_id,
        cast (zipcode as INT) as zipcode,
        cast (country as VARCHAR (26)) as country,
        cast (address as VARCHAR (50)) as address,
        cast (state as VARCHAR (26)) as state,
        cast (CONCAT(zipcode, ' ', country, ' ', address, ' ', state)as STRING) as full_address,
        _fivetran_synced as date_load

    from src_addresses

)

select *
from stg_addresses
