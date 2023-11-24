
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
        cast (user_id as STRING) as user_id,
        cast (address_id as STRING) as address_id ,
        cast (first_name as VARCHAR (50)) as first_name,
        cast (last_name as VARCHAR (50)) as last_name ,
        cast (replace (phone_number,'-','') as NUMBER) as phone_number ,  
        cast (email as VARCHAR (200)) as email ,
        to_date(created_at) as created_at_date ,
        to_time(created_at) as created_at_time,
        to_date(updated_at) as updated_at_date ,
        to_time(updated_at) as updated_at_time,
        cast (total_orders as STRING) as total_orders , -- HAY QUE COMPROBAR PORQUÉ ES NULL 
        cast (_fivetran_synced as timestamp_ntz(9)) as date_load_utc
    FROM src_users
    )

SELECT * FROM stg_users