with src_events as (
    select * from {{ source('sql_server_dbo', 'events') }}
),
tmp_stg_events as (
    select
        cast (event_id as STRING) as event_id,
        cast (page_url as STRING) as page_url,
        cast (event_type as VARCHAR (50)) as event_type,
        cast (user_id as STRING) as user_id,
        cast (session_id as STRING) as session_id,
        to_date(created_at) as created_date,
        to_time(created_at) as created_time,
        cast (_fivetran_synced as timestamp_ntz(9)) as date_load_utc,
        CASE 
            WHEN product_id = '' THEN 'looking_at_products'
            ELSE product_id
        END AS product_id,
        CASE 
            WHEN order_id = '' THEN 'purchased_products'
            ELSE order_id
        END AS order_id
    from src_events
)
select
*
from tmp_stg_events
