{{
    config(
        materialized = "view"
    )
}}

with src_time_day as (
    {{ dbt_date.get_date_dimension('2010-01-01' , '2030-12-31') }}

), 

src_time_day_casted as (
    SELECT *
    FROM src_time_day
)
SELECT top 100 * FROM src_time_day_casted