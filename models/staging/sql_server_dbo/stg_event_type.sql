with src_events as (
    select * from {{ ref('base_events') }}
),

tmp_stg_events as (
    select
        distinct {{dbt_utils.generate_surrogate_key(['event_type'])}} as event_type_key,
        cast (event_type as VARCHAR (50)) as event_type
from src_events
)

select 
*
from tmp_stg_events
