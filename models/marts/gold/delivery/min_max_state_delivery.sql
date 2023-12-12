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
        MAX (order_cost_usd) as maximum_cost ,
        MIN (order_cost_usd) as minimun_cost ,
        status

    FROM stg_orders
    WHERE status in ('delivered', 'shipped','preparing')
    GROUP by status
    )

SELECT * FROM stg_orders_data