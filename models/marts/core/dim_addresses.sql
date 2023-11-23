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
        address_id ,
        address ,
        zipcode , 
        country ,  
        state  
    FROM dim_addresses
    )

SELECT * FROM dim_address_casted