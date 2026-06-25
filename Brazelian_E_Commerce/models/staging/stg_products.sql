{{config(materialized='view')}}

WITH source AS (
    SELECT * FROM {{ source('bronze_products', 'raw_products') }}
),

cleaned AS (
    SELECT
        -- Primary key
        product_id,

        INITCAP(TRIM(COALESCE(product_category_name, 'Unknown'))) AS product_category_name,
        TRY_TO_NUMBER(product_name_length) AS product_name_length,
        TRY_TO_NUMBER(product_description_length) AS product_description_length,
        TRY_TO_NUMBER(producr_photos_qty) AS product_photos_qty,
        TRY_TO_DECIMAL(product_weight_g, 10, 2) AS product_weight_g,
        TRY_TO_DECIMAL(product_length_cm, 10, 2) AS product_length_cm,
        TRY_TO_DECIMAL(product_height_cm, 10, 2) AS product_height_cm,
        TRY_TO_DECIMAL(product_width_cm, 10, 2) AS product_width_cm,

        TRY_TO_DECIMAL(product_length_cm, 10, 2) *
        TRY_TO_DECIMAL(product_height_cm, 10, 2) *
        TRY_TO_DECIMAL(product_width_cm, 10, 2)  AS product_volume_cm3,

        -- Audit columns
        _file_name,
        _loaded_at

    FROM source
    WHERE product_id IS NOT NULL
)

SELECT * FROM cleaned