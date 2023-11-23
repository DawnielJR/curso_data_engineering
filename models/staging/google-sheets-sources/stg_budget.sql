{{
  config(
    materialized='view'
  )
}}

with src_budget as (

    select * from {{ source('google_sheets', 'budget') }}

),

stg_budget as (

    select
        cast (_row as STRING) as budget_id ,
        cast (product_id as STRING) as product_id,
        cast (quantity as INT) as quantity,
        month as month_date,
        dayofmonth(to_date(month)) as month_day,
        cast (_fivetran_synced as timestamp_ntz(9)) as date_load_utc

    from src_budget

)

select * from stg_budget
