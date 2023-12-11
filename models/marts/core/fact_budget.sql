{{
    config(
        materialized='table'
    )
}}

WITH dim_budget_snapshot AS
(
    SELECT *
    FROM {{ ref ('fact_budget_snapshot')}}
)

SELECT 
*
FROM dim_budget_snapshot
where dbt_valid_to is null