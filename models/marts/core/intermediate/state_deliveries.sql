WITH stg_users AS (
    SELECT * 
    FROM {{ ref('stg_users') }}
    ),
    stg_addresses AS (
    SELECT *
    FROM {{ ref('stg_addresses') }}
    ),
    stg_orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
    ),

state_deliveries AS (
    SELECT
        COUNT (a.order_id) as delivery_number,
        b.state
    FROM stg_orders a
    INNER JOIN stg_users c
        ON c.user_id = a.user_id
    INNER JOIN stg_addresses b
        ON b.address_id = c.address_id
    GROUP BY b.state
    )

SELECT * FROM state_deliveries