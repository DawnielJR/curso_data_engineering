{{ config(
  materialized='table'
) }}

WITH stg_order_items AS (
    SELECT *
    FROM {{ ref("stg_date_compl") }}
),

dim_date_time AS (
  {{ dbt_utils.date_spine(
    datepart="hour",
    start_date="'2023-12-07 00:00:00'",
    end_date="'2023-12-07 23:59:59'"
  ) }}
)

SELECT
  date_hour AS timestamp,
  CASE
    WHEN EXTRACT(HOUR FROM date_hour) < 12 THEN 'am'
    ELSE 'pm' END AS am_or_pm,
  EXTRACT(HOUR FROM date_hour) AS hour_24_format,
  EXTRACT(HOUR FROM date_hour) % 12 AS hour_12_format,
  CASE
    WHEN EXTRACT(HOUR FROM CONVERT_TIMEZONE('Europe/Madrid', date_hour)) < 12 THEN 'am'
    ELSE 'pm' END AS am_or_pm_europe,
  CONVERT_TIMEZONE('Europe/Madrid', date_hour) AS hour_timestamp_europe,
  CONVERT_TIMEZONE('America/New_York', date_hour) AS hour_timestamp_america,
  CASE
    WHEN EXTRACT(HOUR FROM CONVERT_TIMEZONE('America/New_York', date_hour)) < 12 THEN 'am'
    ELSE 'pm' END AS am_or_pm_america

FROM dim_date_time
