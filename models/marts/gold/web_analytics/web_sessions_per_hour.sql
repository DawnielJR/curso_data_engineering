    {{
  config(
    materialized='view'
  )
}}
SELECT 
    AVG(unique_session) as avg_unique_session
FROM (
    SELECT 
        DATE_TRUNC('hour', created_time_utc) as hours,
        COUNT(distinct session_id) as unique_session 
    FROM {{ ref('stg_events') }}
    GROUP BY 1
)subquery