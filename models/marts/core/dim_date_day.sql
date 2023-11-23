{{
  config(
    materialized='table'
  )
}}

WITH dim_date_day AS (
    SELECT * 
    FROM {{ ref('stg_date_compl') }}
    ),

dim_date_day_casted AS (
    SELECT
        id_date ,
        day ,
        month ,
        year ,
        week

    FROM dim_date_day
    )

SELECT * FROM dim_date_day_casted