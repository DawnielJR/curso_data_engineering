{{
    config(
        materialized='table'
    )
}}

WITH stg_products AS
(
    SELECT DISTINCT product_id
    FROM {{ ref ('stg_products')}}
),

stg_budget AS
(
    SELECT DISTINCT product_id
    FROM {{ ref('stg_budget')}}
),

stg_events AS
(
    SELECT DISTINCT product_id
    FROM {{ ref('stg_events')}}
),

all_duplicates AS
(
    SELECT *
    FROM stg_products
    UNION ALL
    SELECT *
    FROM stg_budget
    UNION ALL
    SELECT *
    FROM stg_events
),

without_duplicates AS
(
    SELECT DISTINCT (product_id)
    FROM all_duplicates
)

SELECT
*
FROM without_duplicates
FULL JOIN {{ ref('stg_products')}}
USING (product_id)
WHERE product_name is not null