--En promedio, ¿cuánto tiempo tarda un pedido desde que se realiza hasta que se entrega?

{{
  config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

stg_orders AS (
    SELECT 
        AVG(datediff(day,created_at, delivered_at)) as diferencia_dias,
        AVG(datediff(hour, created_at, delivered_at)) as diferencia_horas

    FROM src_orders
    )

SELECT * FROM stg_orders