{{
    config(
        materialized='table'
    )
}}

WITH stg_addresses AS
(
    SELECT DISTINCT address_id
    FROM {{ ref ('stg_addresses')}}
),

stg_users AS
(
    SELECT DISTINCT address_id
    FROM {{ ref('stg_users')}}
),

all_duplicates AS
(
    SELECT *
    FROM stg_addresses
    UNION ALL
    SELECT *
    FROM stg_users
),

without_duplicates AS
(
    SELECT DISTINCT (address_id)
    FROM all_duplicates
)

SELECT
    address_id ,
    address ,
    zipcode ,
    state ,
    country ,
    date_load
FROM without_duplicates
FULL JOIN {{ ref('stg_addresses')}}
USING (address_id)