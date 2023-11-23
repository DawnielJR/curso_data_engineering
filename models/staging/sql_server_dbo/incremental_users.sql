{{ config(
    materialized='incremental',
    unique_key = 'user_id'
    ) 
    }}


WITH stg_users_incremental AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
{% if is_incremental() %}

	  where _fivetran_synced > (select max(F_CARGA) from {{ this }})

{% endif %}
    ),

renamed_casted AS (
    SELECT
        cast (user_id as VARCHAR(50)) as user_id , 
        cast (first_name as VARCHAR (50)) as first_name ,
        cast (last_name as VARCHAR (50)) as last_name ,
        cast (address_id as VARCHAR (50)) as address_id, 
        cast (_fivetran_synced as timestamp_ntz) as F_CARGA,
        cast (replace (phone_number,'-','') as NUMBER) as phone_number  
    FROM stg_users_incremental
    )

SELECT * FROM renamed_casted