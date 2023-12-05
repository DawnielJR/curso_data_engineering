
SELECT *
FROM {{ ref('stg_orders') }}
WHERE  estimated_delivery_date_utc < created_date_utc AND estimated_delivery_time_utc < created_time_utc
