{{ config(
  materialized='table'
) }}


with src_promos as (
    select * from {{ source('sql_server_dbo', 'promos') }}
),

stg_promos as (
    select
        promo_id as promo_type,
        cast(discount as FLOAT) as discount_USD,
        cast (status as VARCHAR(50)) as promo_status,
        cast({{dbt_utils.generate_surrogate_key(['promo_id'])}} as STRING) as promo_id,
        _fivetran_synced as date_load
    from src_promos
)

select 
    promo_type,
    discount_USD,
    promo_status,
    promo_id
from stg_promos
union all 
SELECT 
    'sin_promo' ,
    0 , 
    'inactive' ,
    {{dbt_utils.generate_surrogate_key(['9999'])}} 


