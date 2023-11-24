---3. ¿Cuántos usuarios han realizado una sola compra? ¿Dos compras? ¿Tres o más compras?

WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

stg_orders AS (
    SELECT 
        user_id,
        COUNT (DISTINCT order_id) as pedidos_realizados
    FROM src_orders
    GROUP BY 1
),
user_per_buying as 
(
SELECT 
    CASE 
        WHEN pedidos_realizados >=3 THEN '3+'
        ELSE  cast (pedidos_realizados as VARCHAR) 
        END AS pedidos_realizados,
    COUNT (user_id) as num_users
FROM stg_orders
GROUP BY 1
)
SELECT * FROM user_per_buying