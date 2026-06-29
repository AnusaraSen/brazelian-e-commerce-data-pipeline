
{% snapshot snap_customer %}

{{
    config(
        target_schema='SNAPSHOTS',
        target_database='SILVER_DB',
        unique_key='customer_id',
        strategy='check',
        check_cols=[
            'customer_city',
            'customer_state',
            'customer_zip_code_prefix'
        ],
        invalidate_hard_deletes=True
    )
}}

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    _loaded_at
FROM {{ source('bronze_customers', 'raw_customers') }}

{% endsnapshot %}