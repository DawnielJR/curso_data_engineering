WITH stg_users AS (
    SELECT * 
    FROM {{ ref('stg_users') }}
    ),
    stg_orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
    ),
    stg_addresses AS (
    SELECT *
    FROM {{ ref('stg_addresses') }}
    ),
    stg_promos AS (
    SELECT *
    FROM {{ ref('stg_promos')}}
    ),
state_users_sales AS (
    SELECT
        b.first_name ,
        b.last_name ,
        SUM(c.order_total) as total_import ,
        d.discount 
    FROM stg_addresses a
    INNER JOIN stg_users b
        ON a.address_id = b.address_id
    INNER JOIN stg_orders c
        ON b.user_id = c.user_id
    INNER JOIN stg_promos d
        ON c.promo_id = d.promo_id
    GROUP BY b.first_name , b.last_name , d.discount
    ORDER BY total_import DESC
    )

SELECT * FROM state_users_sales