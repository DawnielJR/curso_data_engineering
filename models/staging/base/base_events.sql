with src_events as (
    select * from {{ source('sql_server_dbo', 'events') }}
),
base_events as (
    select
        cast (event_id as VARCHAR (128)) as event_id,
        cast (page_url as STRING) as page_url,
        cast (event_type as VARCHAR (128)) as event_type,
        CASE 
            WHEN product_id='' then null
            ELSE product_id
            END AS product_id ,
        cast (user_id as VARCHAR (128)) as user_id,
        cast (session_id as VARCHAR (128)) as session_id,
        to_date(created_at) as created_date_utc,
        to_time(created_at) as created_time_utc,
        CASE 
            WHEN order_id='' then 'no_order_id'
            ELSE order_id
            END AS order_id,
        cast (_fivetran_synced as timestamp_ntz(9)) as date_load_utc
    from src_events
)
select
*
from base_events
