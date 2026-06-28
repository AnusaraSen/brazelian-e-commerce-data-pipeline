{{ config(materialized='table') }}

WITH sellers AS (
    SELECT * FROM {{ ref('stg_sellers') }}  
),

geolocation AS (
    SELECT * FROM {{ ref('dim_geolocation') }} 
),

final AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['seller_id']) }}  AS seller_key,

        -- Business keys
        s.seller_id,    -- seller identifier

        -- Seller location from stg_sellers
        s.seller_zip_code_prefix,
        s.seller_city,
        s.seller_state,
        

        -- Enriched coordinates from dim_geolocation
        -- LEFT JOIN means these will be NULL if no zip match found
        g.latitude,
        g.longitude,

        -- choose the city from the geolocation dimension, not the staging model because the geolocation dimension has been cleaned and standardized
        g.city AS primary_city   

    FROM sellers s
    LEFT JOIN geolocation g
        ON s.seller_zip_code_prefix = g.zip_code_prefix   
)

SELECT * FROM final