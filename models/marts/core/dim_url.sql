{{
  config(
    materialized='table'
  )
}}

WITH dim_url AS (
    SELECT * 
    FROM {{ ref('stg_events') }}
    ),

dim_url_casted AS (
    SELECT
        event_id ,
        session_id ,
        page_url  
    FROM dim_url
    )

SELECT * FROM dim_url_casted