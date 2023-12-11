{{
  config(
    materialized='view'
  )
}}

WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_orders') }}
    ),

stg_orders_data AS (
    SELECT 
        AVG(datediff(day,created_date_utc, delivered_date_utc)) as diferencia_dias,
        AVG(datediff(hour, created_time_utc, delivered_time_utc)) as diferencia_horas

    FROM stg_orders
    )

SELECT * FROM stg_orders_data