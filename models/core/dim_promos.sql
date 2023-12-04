{{
  config(
    materialized='table'
  )
}}

WITH dim_promos AS (
    SELECT * 
    FROM {{ ref('stg_promos') }}
    ),

dim_promos_casted AS (
    SELECT
        promo_key ,
        promo_type ,
        promo_status 
    FROM dim_promos
    )

SELECT * FROM dim_promos_casted