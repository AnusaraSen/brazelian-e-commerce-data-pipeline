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

WITH source AS (
    SELECT * FROM {{ source('bronze_customers', 'raw_customers') }}
),

-- Get only the latest record per customer_id
-- This is what the snapshot should compare against
deduped AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY _loaded_at DESC
    ) = 1
)

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    _loaded_at
FROM deduped

{% endsnapshot %}