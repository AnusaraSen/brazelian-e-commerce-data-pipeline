{{ config(materialized='table') }}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}  
),

geolocation AS (
    SELECT * FROM {{ ref('dim_geolocation') }} 
),

final AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }}  AS customer_key,

        -- Business keys
        c.customer_id,    -- per-order customer identifier
        c.customer_unique_id,    -- real person identifier

        -- Customer location from stg_customers
        c.zip_code_prefix,
        c.customer_city,
        c.customer_state,
        

        -- Enriched coordinates from dim_geolocation
        -- LEFT JOIN means these will be NULL if no zip match found
        g.latitude,
        g.longitude,

        -- choose the city from the geolocation dimension, not the staging model because the geolocation dimension has been cleaned and standardized
        g.city AS primary_city,   

    FROM customers c
    LEFT JOIN geolocation g
        ON c.zip_code_prefix = g.zip_code_prefix   
)

SELECT * FROM final