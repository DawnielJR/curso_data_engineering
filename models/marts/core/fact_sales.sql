{{
    config(
        materialized='table'
    )
}}

WITH fact_sales_snapshot AS
(
    SELECT *
    FROM {{ ref ('fact_sales_snapshot')}}
)

SELECT 
*
FROM fact_sales_snapshot
where dbt_valid_to is null