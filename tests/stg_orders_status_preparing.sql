SELECT *
FROM {{ ref('stg_orders') }} 
WHERE status = 'preparing'  
AND estimated_delivery_date_utc is not null 
AND estimated_delivery_time_utc is not null 
AND delivered_date_utc is not null
AND delivered_time_utc is not null  
AND tracking_id is not null 
AND shipping_service !=''