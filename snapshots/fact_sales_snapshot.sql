{%snapshot fact_sales_snapshot %}
{{
    config(
        target_schema='snapshot' ,
        unique_key = 'order_item_id' ,
        strategy = 'timestamp' ,
        updated_at = 'date_load_utc'
    )
}}


WITH stg_order_items AS (
    SELECT *
    FROM {{ ref("stg_order_items") }}
),

stg_products AS (
    SELECT 
        product_id,
        price_usd
    FROM {{ ref("stg_products") }}
),

stg_orders AS (
    SELECT *
    FROM {{ ref("stg_orders") }}
),

stg_orders_quantity AS (
    SELECT 
        order_id, 
        SUM(quantity) AS total_quantity
    FROM {{ ref('stg_orders') }} 
    JOIN {{ ref('stg_order_items') }} 
    USING(order_id)
    GROUP BY order_id
),

union_order_items AS (
    SELECT
        order_item_id,
        order_id,
        user_id,
        address_id,
        created_date_utc,
        created_time_utc,
        product_id,
        quantity AS quantity_products,
        total_quantity AS total_quantity_order_item,
        price_usd AS price_unit_product_usd,
        (price_usd * quantity) AS price_total_order_item_usd,
        shipping_service_id,
        (shipping_cost_usd * (quantity / total_quantity_order_item))::decimal(7,3) AS shipping_cost_item_usd,
        promo_id,
        status_id,
        estimated_delivery_date_utc,
        estimated_delivery_time_utc,
        delivered_date_utc,
        delivered_time_utc,
        tracking_id,
        date_load_utc
    FROM stg_order_items
    JOIN stg_orders
    USING(order_id)
    JOIN stg_products
    USING(product_id)
    JOIN stg_orders_quantity
    USING(order_id)
    ORDER BY order_id
)
   
SELECT
    order_item_id,
    order_id,
    user_id,
    address_id,
    created_date_utc,
    created_time_utc,
    product_id,
    quantity_products,
    total_quantity_order_item,
    price_unit_product_usd,
    price_total_order_item_usd,
    shipping_service_id,
    shipping_cost_item_usd,
    promo_id,
    status_id,
    estimated_delivery_date_utc,
    estimated_delivery_time_utc,
    delivered_date_utc,
    delivered_time_utc,
    tracking_id,
    date_load_utc

FROM union_order_items

{% endsnapshot %}