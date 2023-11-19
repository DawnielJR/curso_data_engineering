WITH stg_users AS (
    SELECT * 
    FROM {{ ref('stg_users') }}
    ),
    stg_addresses AS (
    SELECT *
    FROM {{ ref('stg_addresses') }}
),

addresses_users AS (
    SELECT
        p.user_id
        , p.first_name
        , p.last_name
        , p.email
        , p.phone_number
        , p.address_id
        , s.zipcode
        , s.country
        , s.address
        , s.state
    FROM stg_users p
    INNER JOIN stg_addresses s
    ON s.address_id = p.address_id
    )

SELECT * FROM addresses_users