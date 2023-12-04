{{
  config(
    materialized='table'
  )
}}

WITH dim_transport AS (
    SELECT * 
    FROM {{ ref('stg_orders') }}
    ),

dim_transport_casted AS (
    SELECT
        tracking_key ,
        shipping_service 
    FROM dim_transport
    )

SELECT * FROM dim_transport_casted