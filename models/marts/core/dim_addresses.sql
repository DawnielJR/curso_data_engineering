{{
  config(
    materialized='table'
  )
}}

WITH dim_addresses AS (
    SELECT * 
    FROM {{ ref('stg_addresses') }}
    ),

dim_address_casted AS (
    SELECT
        address_key ,
        zipcode ,
        country, 
        address ,
        state ,  
        date_load 
    FROM dim_addresses
    )

SELECT * FROM dim_address_casted