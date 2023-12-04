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
        event_key ,
        session_key ,
        page_url  
    FROM dim_url
    )

SELECT * FROM dim_url_casted