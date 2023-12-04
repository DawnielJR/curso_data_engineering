{{
  config(
    materialized='table'
  )
}}

WITH stg_order_items AS 
(
    SELECT *
    FROM {{ ref("stg_order_items") }}
),

stg_orders AS 
(
    SELECT *
    FROM {{ ref("stg_orders") }}
),

stg_products AS 
(
    SELECT 
        price_usd,
        inventory
    FROM {{ ref("stg_products") }}
),

dim_products  AS
(
    SELECT *
    FROM {{ ref ("dim_products")}}

),

dim_users AS
(
    SELECT *
    FROM {{ ref ("dim_users")}}
),

dim_addresses AS
(
    SELECT *
    FROM {{ ref ("dim_addresses")}}
),


sales_items AS (
    SELECT
        a.order_sk, --id de order_items
        b.order_key, --id de orders
        user_key,
        created_at_utc,
        a.product_key,
        a.quantity,
        c.inventory , 
        c.price_usd AS order_cost_item_usd,
        (c.price_usd / b.order_total_usd) * b.shipping_cost_usd AS shipping_cost_item_usd,
        b.status,
        b.shipping_service,
        address_key,
        b.estimated_delivery_date_utc,
        b.estimated_delivery_time_utc,
        b.delivered_date_utc,
        b.delivered_time_utc,
        b.tracking_key,
        b.promo_key
    FROM stg_order_items a
    FULL JOIN stg_orders b
        ON a.order_sk = b.order_key
    FULL JOIN stg_products c
        ON a.product_key = c.product_key
)
   
SELECT *
    /*order_item_sk,
    order_key,
    created_date_key,
    product_id,
    quantity,
    order_cost_item_usd::DECIMAL(7,2) AS order_cost_item_usd,
    shipping_cost_item_usd::DECIMAL(7,2) AS shipping_cost_item_usd,
    user_key,
    status_key,
    shipping_service_key,
    address_key,
    estimated_delivery_date_utc_key,
    estimated_delivery_time_utc_key,
    delivered_date_utc_key,
    delivered_time_utc_key,
    tracking_id,
    promo_key*/
FROM sales_items