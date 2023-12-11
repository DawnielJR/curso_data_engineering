{%snapshot fact_budget_snapshot %}
{{
    config(
        target_schema='snapshot' ,
        unique_key = 'budget_id' ,
        strategy = 'timestamp' ,
        updated_at = 'date_load_utc'
    )
}}
WITH stg_budget AS 
(
    SELECT *
    FROM {{ ref('stg_budget') }}
)

SELECT *
FROM stg_budget

{% endsnapshot %}