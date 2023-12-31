with src_events as (
    select * from {{ ref( 'base_events') }}
),
tmp_stg_events as (
    select
        event_id,
        page_url,
        event_type,
        product_id , 
        user_id,
        session_id,
        created_date,
        created_time,
        order_id,
        date_load_utc
    from src_events
)
select
*
from tmp_stg_events
