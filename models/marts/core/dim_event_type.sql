{{
  config(
    materialized='table'
  )
}}

WITH dim_event_type AS (
    SELECT * 
    FROM {{ ref('stg_event_type') }}
    ),

dim_event_type_casted AS (
    SELECT
    event_type_id,
    event_type
    FROM dim_event_type
    )

SELECT * FROM dim_event_type_casted