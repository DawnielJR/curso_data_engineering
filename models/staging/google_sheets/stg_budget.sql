{{
  config(
    materialized='incremental' ,
    unique_key = 'budget_id'
  )
}}

with src_budget as (

    select * from {{ source('google_sheets', 'budget') }}

),

stg_budget as (

    select
        {{ dbt_utils.generate_surrogate_key(['_row']) }} AS budget_id,
        cast (product_id as VARCHAR (128)) as product_id,
        cast (quantity as INTEGER) as quantity,
        month as month_date,
        dayofmonth(to_date(month)) as month_day,
        cast (_fivetran_synced as timestamp_ntz(9)) as date_load_utc

    from src_budget

{% if is_incremental()%}
where _fivetran_synced > (select max (date_load_utc) from {{this}})
{%endif%}
)

select * from stg_budget
