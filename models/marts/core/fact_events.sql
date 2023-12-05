{{
    config(
        materialized='table'
    )
}}

WITH stg_events AS
(
    SELECT *
    FROM {{ ref ('stg_events')}}
)

SELECT *
FROM stg_events