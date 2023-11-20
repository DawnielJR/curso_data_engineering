{{ config(
  materialized='table'
) }}


with src_promos as (
    select * from {{ source('sql_server_dbo', 'promos') }}
),

stg_promos as (
    select
        UPPER(promo_id) as promo_type,
        cast(discount as FLOAT) as discount_USD,
        cast (status as VARCHAR(50)) as promo_status,
        {{dbt_utils.generate_surrogate_key(['promo_id'])}} as promo_sk,
        _fivetran_synced as date_load
    from src_promos
)

select 
    promo_type,
    discount_USD,
    promo_status,
    promo_sk
from stg_promos
union all 
SELECT 'SIN_PROMO', 0, 'inactive', '9999' 


