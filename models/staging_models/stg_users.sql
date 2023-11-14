
{{
  config(
    materialized='table'
  )
}}

WITH src_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

stg_users AS (
    SELECT
        user_id::VARCHAR(256) ,
        updated_at::TIMESTAMP_TZ(9) ,
        address_id::VARCHAR(256) ,
        last_name::VARCHAR(256) ,
        created_at::TIMESTAMP_TZ(9) ,
        phone_number::VARCHAR(40) ,
        total_orders::NUMBER(38,0) ,
        first_name::VARCHAR(60) ,
        email::VARCHAR(256) ,
        _fivetran_deleted::BOOLEAN,
        _fivetran_synced::TIMESTAMP_TZ (9) AS date_load
    FROM src_users
    )

SELECT * FROM stg_users