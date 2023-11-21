
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
        user_id ,
        updated_at ,
        address_id ,
        last_name ,
        created_at ,
        phone_number ,
        total_orders ,
        first_name ,
        email ,
        _fivetran_deleted as date_deleted,
        _fivetran_synced AS date_load
    FROM src_users
    )

SELECT * FROM stg_users