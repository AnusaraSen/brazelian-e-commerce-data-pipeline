{{config(materialized='view')}}

WITH source AS (
    SELECT * FROM {{ source('bronze_category', 'raw_product_category_name_translation') }}
),

cleaned AS (
    SELECT
        
       INITCAP(TRIM(product_category)) AS product_category_name,
       INITCAP(TRIM(product_category_name_english)) AS product_category_name_english,

        -- Audit columns
        _file_name,
        _loaded_at

    FROM source
    WHERE product_category_name IS NOT NULL
)

SELECT * FROM cleaned