{{ config(
  materialized='table'
) }}

WITH dim_date_month AS 
(
    SELECT *
    FROM {{ ref ('stg_date_compl')}}
)

SELECT
    date ,
    day_of_week , 
    day_of_month ,
    week_of_year , 
    month_of_year , 
    year_number 
FROM dim_date_month