{{
    config(
        materialized='table'
    )
}}

WITH dim_users_snapshot AS
(
    SELECT *
    FROM {{ ref ('dim_users_snapshot')}}
)

SELECT 
    user_id ,
    first_name ,
    last_name ,
    address_id ,
    email ,
    phone_number ,
    created_date_utc ,
    created_time_utc ,
    updated_at_date ,
    updated_at_time
FROM dim_users_snapshot
where dbt_valid_to is null
 
