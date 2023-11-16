
{{
  config(
    materialized='table'
  )
}}


with src_promos as (

    select * from {{ source('sql_server_dbo', 'promos') }}

),

stg_promos as (

    select
        promo_id,
        discount,
        status as promo_status,
        _fivetran_deleted as date_deleted,
        _fivetran_synced as date_load

    from src_promos

)

select * from stg_promos
