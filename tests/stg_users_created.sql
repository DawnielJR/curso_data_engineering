SELECT *
FROM {{ ref('stg_users') }}
WHERE updated_at_date < created_date_utc AND updated_at_time < created_time_utc