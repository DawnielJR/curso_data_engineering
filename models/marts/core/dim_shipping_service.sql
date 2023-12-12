{{ config(
  materialized='table'
) }}

WITH shipping_services AS (
    SELECT DISTINCT
        shipping_service_id,
        shipping_service,
        SUM(shipping_cost_usd) AS total_shipping_cost_usd,
        COUNT(shipping_service) AS total_shipping_service,
        AVG(shipping_cost_usd) AS avg_shipping_service_cost

    FROM {{ ref('stg_orders') }}
    GROUP BY shipping_service_id, shipping_service
),

most_common_shipment_days AS (
    SELECT
        shipping_service_id,
        ARRAY_AGG(DISTINCT EXTRACT(DOW FROM estimated_delivery_date_utc)) AS most_common_shipment_days

    FROM {{ ref('stg_orders') }}
    WHERE shipping_service IS NOT NULL
    GROUP BY shipping_service_id
)

SELECT
    ss.*,
    mcs.most_common_shipment_days


FROM shipping_services ss

LEFT JOIN most_common_shipment_days mcs
USING (shipping_service_id)
