{{ config(
    materialized='incremental',
    unique_key='sales_key',
    on_schema_change='sync_all_columns'
) }}

{% if is_incremental() %}
    {% set max_loaded_at_query %}
        SELECT COALESCE(MAX(_loaded_at), '1900-01-01'::TIMESTAMP)
        FROM {{ this }}
    {% endset %}
    {% set max_loaded_at = run_query(max_loaded_at_query).columns[0].values()[0] %}
{% endif %}

WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
    {% if is_incremental() %}
        WHERE _loaded_at > '{{ max_loaded_at }}'
    {% endif %}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

payments AS (
    SELECT * FROM {{ ref('payments_aggregated') }}
),

dim_customers AS (
    SELECT * FROM {{ ref('dim_customer') }}
),

dim_sellers AS (
    SELECT * FROM {{ ref('dim_seller') }}
),

dim_products AS (
    SELECT * FROM {{ ref('dim_product') }}
),

dim_dates AS (
    SELECT * FROM {{ ref('dim_date') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'oi.order_id',
            'oi.order_item_id'
        ]) }}                                   AS sales_key,

        dc.customer_key,
        ds.seller_key,
        dp.product_key,
        dd.date_key,

        oi.order_id,
        oi.order_item_id,

        o.order_status,
        o.order_purchase_timestamp,
        o.delivered_on_time,
        o.days_to_deliver,

        oi.price,
        oi.freight_value,
        oi.order_item_total,
        p.total_payment_value,
        COALESCE(p.primary_payment_type, 'Unknown') AS primary_payment_type,
        p.is_mixed_payment,

        oi._loaded_at

    FROM order_items oi
    LEFT JOIN orders o
        ON oi.order_id = o.order_id
    LEFT JOIN payments p
        ON oi.order_id = p.order_id
    LEFT JOIN dim_customers dc
        ON o.customer_id = dc.customer_id
    LEFT JOIN dim_sellers ds
        ON oi.seller_id = ds.seller_id
    LEFT JOIN dim_products dp
        ON oi.product_id = dp.product_id
    LEFT JOIN dim_dates dd
        ON CAST(o.order_purchase_timestamp AS DATE) = dd.full_date
    WHERE o.order_status IN ('delivered', 'shipped')
)

SELECT * FROM final