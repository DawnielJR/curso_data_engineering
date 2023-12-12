
SELECT *
FROM {{ ref('stg_orders') }}
WHERE delivered_date_utc < created_date_utc AND delivered_time_utc < created_time_utc

