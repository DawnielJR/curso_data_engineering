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
        product_id,
        price_usd
    FROM {{ ref("stg_products") }}
),

sales_items AS (
    SELECT
        order_item_sk,
        order_key,
        user_key,
        created_date_key,
        product_id,
        quantity,
        price_usd AS order_cost_item_usd,
        (price_usd / order_total_usd) * shipping_cost_usd AS shipping_cost_item_usd,
        status_key,
        shipping_service_key,
        address_key,
        estimated_delivery_date_utc_key,
        estimated_delivery_time_utc_key,
        delivered_date_utc_key,
        delivered_time_utc_key,
        tracking_id,
        promo_key
    FROM stg_order_items a
    JOIN stg_orders b
        ON a.order_sk = b.order_key
    JOIN stg_products c
        ON a.product_sk = c.product_id
)
   
SELECT
    order_item_sk,
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
    promo_key
FROM sales_items