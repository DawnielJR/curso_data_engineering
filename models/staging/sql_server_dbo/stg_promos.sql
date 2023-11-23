{{ config(
  materialized='view'
) }}


with src_promos as (
    select * from {{ source('sql_server_dbo', 'promos') }}
),

stg_promos as (
    select
        cast (promo_id as VARCHAR (30)) as promo_type,
        cast(discount as FLOAT) as discount_usd,
        cast (status as VARCHAR(50)) as promo_status,
        cast({{dbt_utils.generate_surrogate_key(['promo_id'])}} as STRING) as promo_id,
        cast (_fivetran_synced as timestamp_ntz(9)) as date_load_utc
    from src_promos
)

select 
    promo_type,
    discount_usd,
    promo_status,
    promo_id
from stg_promos
union all 
SELECT 
    'sin_promo' ,
    0 , 
    'inactive' ,
    {{dbt_utils.generate_surrogate_key(['9999'])}} 


