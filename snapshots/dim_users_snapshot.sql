{%snapshot dim_users_snapshot %}
{{
    config(
        target_schema='snapshot' ,
        unique_key = 'user_id' ,
        strategy = 'timestamp' ,
        updated_at = 'date_load_utc'
    )
}}

WITH stg_orders AS
(
    SELECT DISTINCT user_id
    FROM {{ref ('stg_orders')}}
),

stg_users AS
(
    SELECT DISTINCT user_id
    FROM {{ref ('stg_users')}}
),

stg_events AS
(
    SELECT DISTINCT user_id
    FROM {{ref ('stg_events')}}
),

all_duplicates_users AS 
(
    SELECT *
    FROM stg_users
    UNION ALL
    SELECT *
    FROM stg_events
    UNION ALL
    SELECT *
    FROM stg_orders
),

without_duplicates AS(
    SELECT DISTINCT user_id
    FROM all_duplicates_users
)

SELECT *
FROM without_duplicates
FULL JOIN {{ ref('stg_users')}}
USING (user_id)

{% endsnapshot %}