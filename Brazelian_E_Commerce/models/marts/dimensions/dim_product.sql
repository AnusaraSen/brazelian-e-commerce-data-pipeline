{{ config(materialized='table') }}

WITH product AS (
    SELECT * FROM {{ ref('stg_products') }}
),

category_translation AS (
    SELECT * FROM {{ ref('stg_category_translation') }}
),

final AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,

        -- Business key
        p.product_id,

        -- Category in both languages
        p.product_category_name AS product_category_name_portuguese,
        COALESCE(
            ct.product_category_name_english,
            p.product_category_name,
            'Uncategorized'
        )AS product_category_name_english,

        -- Product characteristics
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,

        -- Physical dimensions
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,
        p.product_volume_cm3

    FROM product p
    LEFT JOIN category_translation ct
        ON p.product_category_name = ct.product_category_name
)

SELECT * FROM final