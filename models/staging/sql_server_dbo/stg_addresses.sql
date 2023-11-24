{{ config(materialized="view") }}

with
src_addresses as (select * from {{ source("sql_server_dbo", "addresses") }}),

stg_addresses as (

    select
        cast (address_id as STRING) as address_id,
        cast (zipcode as STRING) as zipcode,
        cast (country as STRING) as country,
        cast (address as STRING) as address,
        cast (state as STRING) as state,
        _fivetran_synced as date_load

    from src_addresses

)

select 
    {{ dbt_utils.generate_surrogate_key(['address_id' , 'zipcode' ]) }} AS address_key ,
    zipcode,
    country,
    address,
    state,
    date_load
from stg_addresses
