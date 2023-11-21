{% snapshot users_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='user_id',
        strategy='check',
        check_cols=['first_name','last_name','address_id','phone_number'],
        invalidate_hard_deletes=True,
    )
}}

select * from {{ ref( 'incremental_users') }}
where F_CARGA = (select max(F_CARGA) from {{ 'incremental_users' }})

{% endsnapshot %}