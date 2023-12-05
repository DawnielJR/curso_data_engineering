{{
    config(
        materialized='table'
    )
}}

WITH stg_event_type AS
(
    SELECT *
    FROM {{ ref ('stg_event_type')}}
)

SELECT *
FROM stg_event_type