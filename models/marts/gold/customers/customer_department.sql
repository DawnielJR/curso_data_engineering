{{
  config(
    materialized='view'
  )
}}

WITH stg_users AS (
    SELECT * 
    FROM {{ ref('stg_users') }}
    ),

stg_users_data AS (
    SELECT 
        COUNT(user_id) as numero_usuarios
    FROM stg_users
    )

SELECT * FROM stg_users_data