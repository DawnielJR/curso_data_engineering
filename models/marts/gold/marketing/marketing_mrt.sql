{{
  config(
    materialized='view'
  )
}}

WITH mrt_order_products_state AS (
    SELECT * 
    FROM {{ ref('stg_orders') }}
),

mrt_address_products_state AS (
    SELECT *
    FROM {{ ref ('stg_addresses') }}
)

SELECT
    state,
    COUNT(*) as total_orders
FROM mrt_order_products_state
INNER JOIN mrt_address_products_state
USING (address_id)
GROUP BY state
ORDER BY total_orders DESC