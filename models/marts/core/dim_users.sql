{{
    config(
        materialized='table'
    )
}}
WITH stg_orders AS
(
    SELECT DISTINCT user_id
    FROM {{ref ('stg_orders')}}
),

stg_users AS
(
    SELECT DISTINCT user_id
    FROM {{ref ('stg_users')}}
),

stg_events AS
(
    SELECT DISTINCT user_id
    FROM {{ref ('stg_events')}}
),

all_duplicates_users AS 
(
    SELECT *
    FROM stg_users
    UNION ALL
    SELECT *
    FROM stg_events
    UNION ALL
    SELECT *
    FROM stg_orders
),

without_duplicates AS(
    SELECT DISTINCT user_id
    FROM all_duplicates_users
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
FROM without_duplicates
FULL JOIN {{ ref('stg_users')}}
USING (user_id)