---¿Cuántos usuarios tenemos?

{{
  config(
    materialized='view'
  )
}}

WITH src_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

stg_users AS (
    SELECT 
        COUNT(user_id) as numero_usuarios
    FROM src_users
    )

SELECT * FROM stg_users

