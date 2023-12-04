{{
  config(
    materialized='view' , 
    unique_key='promo_id'
  )
}}


with src_promos as (
    select * from {{ source('sql_server_dbo', 'promos') }}
),

stg_promos as (
    SELECT
        cast (promo_id as VARCHAR (128)) as promo_id,
        cast(discount as FLOAT) as discount_usd,
        cast (status as VARCHAR(50)) as promo_status,
        cast (_fivetran_synced as timestamp_ntz(9)) as date_load_utc
    FROM src_promos
    UNION ALL
    
    SELECT 
    'sin promo',
    0,
    'inactive',
    '2023-12-12 12:00:00.244000'
),

stg_promos_casted as(
    SELECT
        {{ dbt_utils.generate_surrogate_key(['promo_id']) }} AS promo_id ,
        promo_id AS promo_name ,
        discount_usd ,
        promo_status ,
        date_load_utc
    FROM stg_promos
)
SELECT * FROM stg_promos_casted

