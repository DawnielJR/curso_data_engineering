SELECT *
FROM {{ ref('stg_orders') }} 
WHERE status = 'delivered'  
AND estimated_delivery_date_utc is null 
AND delivered_date_utc is null
AND estimated_delivery_time_utc is null 
AND delivered_time_utc is null  
AND tracking_id ='' 
AND shipping_service=''