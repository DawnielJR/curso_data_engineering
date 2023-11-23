with src_events as (
    select * from {{ source('sql_server_dbo', 'events') }}
),
base_events as (
    select
        cast (event_id as STRING) as event_id,
        cast (page_url as STRING) as page_url,
        cast (event_type as VARCHAR (50)) as event_type,
        decode (product_id, '', 'no_product', null, 'no_product', product_id ) as product_id , 
        cast (user_id as STRING) as user_id,
        cast (session_id as STRING) as session_id,
        to_date(created_at) as created_date,
        to_time(created_at) as created_time,
        decode (order_id, '' , 'no_order', null, 'no_order', order_id) as order_id,
        cast (_fivetran_synced as timestamp_ntz(9)) as date_load_utc
    from src_events
)
select
*
from base_events
