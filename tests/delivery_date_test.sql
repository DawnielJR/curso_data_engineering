SELECT *
FROM {{ ref('stg_orders') }}
WHERE delivered_date_utc < created_at_utc
