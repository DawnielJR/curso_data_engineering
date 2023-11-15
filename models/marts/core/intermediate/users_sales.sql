WITH stg_users AS (
    SELECT * 
    FROM {{ ref('stg_users') }}
    ),
    stg_orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
),

user_sales AS (
    SELECT
        a.first_name ,
        a.last_name ,
        a.email ,
        a.phone_number ,
        SUM(b.order_cost) as total_cost
    FROM stg_users a
    INNER JOIN stg_orders b
    ON a.user_id = b.user_id
    GROUP BY a.first_name , a.last_name , a.email , a.phone_number
    ORDER BY total_cost DESC
    )

SELECT * FROM user_sales