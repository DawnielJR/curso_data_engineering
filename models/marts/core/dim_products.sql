{{
  config(
    materialized='table'
  )
}}

WITH dim_products AS (
    SELECT * 
    FROM {{ ref('stg_products') }}
    ),

dim_products_casted AS (
    SELECT
        product_id ,
        name
    FROM dim_products
    )

SELECT * FROM dim_products_casted