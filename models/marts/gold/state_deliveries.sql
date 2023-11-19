WITH int_addresses_users AS (
    SELECT * 
    FROM {{ ref('int_addresses_users') }}
    ),
    stg_orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
    ),

state_deliveries AS (
    SELECT
        COUNT (a.order_id) as delivery_number,
        c.state
    FROM stg_orders a
    INNER JOIN int_addresses_users c
        ON c.user_id = a.user_id
    GROUP BY c.state
    )

SELECT * FROM state_deliveries