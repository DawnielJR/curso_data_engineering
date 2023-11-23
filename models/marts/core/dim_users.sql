{{
  config(
    materialized='table'
  )
}}

WITH dim_users AS (
    SELECT * 
    FROM {{ ref('stg_users') }}
    ),

dim_users_casted AS (
    SELECT
        user_id ,
        address_id ,
        CONCAT(first_name, ' ', last_name) as full_name , 
        phone_number ,  
        email 
    FROM dim_users
    )

SELECT * FROM dim_users_casted