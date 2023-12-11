{{
  config(
    materialized='incremental' ,
    unique_key = 'product_id'
  )
}}

with src_products as (
SELECT  
    cast(product_id as VARCHAR (128)) as product_id,
    cast(price as FLOAT) as price_usd,
    cast(name as VARCHAR (128)) as product_name,
    cast(inventory as INTEGER) as inventory,
    cast(_fivetran_synced as timestamp_ntz(9)) as date_load_utc 
FROM {{ source('sql_server_dbo', 'products') }}
UNION ALL
SELECT
    null AS product_id ,
    '0' AS price_usd ,
    'Without Products' AS product_name ,
    '0' AS inventory ,
    '2023-12-12 12:00:00.851000' AS date_load_utc
{% if is_incremental()%}
where _fivetran_synced > (select max (date_load_utc) from {{this}})
{%endif%}

),

products_casted AS (
    SELECT 
           product_id
        ,  product_name
        ,  price_usd
        ,  inventory
        ,  date_load_utc

    FROM src_products
    )

SELECT * FROM products_casted