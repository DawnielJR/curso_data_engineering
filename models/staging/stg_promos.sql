
{{
  config(
    materialized='table',
    unique_key = 'promo_id_subrogated'
  )
}}


with src_promos as (

    select * from {{ source('sql_server_dbo', 'promos') }}

),

stg_promos as (

    select
        promo_id as promo_type,
        discount,
        status as promo_status,
        MD5(CAST(promo_id AS STRING) || CAST(discount AS STRING) || COALESCE(promo_status, '')) AS promo_id_subrogated,
        _fivetran_synced as date_load

    from src_promos

)
    SELECT 
    promo_type,
    discount,
    promo_status,
    promo_id_subrogated
    FROM stg_promos
    GROUP BY 1,2,3,4
    HAVING COUNT (*) = 1

