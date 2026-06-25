{{config(materialized='view')}}

WITH source AS (
    SELECT * FROM {{ source('bronze_sellers', 'raw_sellers') }}
),

cleaned AS (
    SELECT
        -- Primary key
        seller_id, --Used to connect the orders table to the sellers table

        --seller_zip_code_prefix — should be exactly 5 digits
        LPAD(TRIM(seller_zip_code_prefix), 5, '0') AS seller_zip_code_prefix,
    
        --seller_city — should be in title case (first letter of each word capitalized)
        INITCAP(TRIM(COALESCE(seller_city,'Unknown'))) AS seller_city,

        -- seller_state — should be exactly 2 uppercase letters
        UPPER(TRIM(COALESCE(seller_state, 'XX'))) AS seller_state,
        

        -- Audit columns
        _file_name,
        _loaded_at

    FROM source
    WHERE seller_id IS NOT NULL
)

SELECT * FROM cleaned