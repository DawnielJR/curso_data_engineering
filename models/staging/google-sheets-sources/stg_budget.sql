{{
  config(
    materialized='table'
  )
}}

with src_budget as (

    select * from {{ source('google_sheets', 'budget') }}

),

stg_budget as (

    select
        _row as budget_id ,
        product_id,
        quantity,
        month,
        _fivetran_synced as date_load

    from src_budget

)

select * from stg_budget
