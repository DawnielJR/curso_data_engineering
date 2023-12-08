{{
  config(
    materialized='table'
  )
}}

WITH stg_events AS 
(
    SELECT *
    FROM {{ ref("stg_events") }}
)

SELECT
    event_id ,
    event_type_id ,
    user_id ,
    session_id ,
    order_id ,
    product_id ,
    created_date_utc ,
    created_time_utc
FROM stg_events