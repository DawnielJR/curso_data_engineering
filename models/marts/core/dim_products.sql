{{
    config(
        materialized='table'
    )
}}

WITH dim_products_snapshot AS
(
    SELECT *
    FROM {{ ref ('dim_products_snapshot')}}
)

SELECT 
*
FROM dim_products_snapshot
where dbt_valid_to is null
 
