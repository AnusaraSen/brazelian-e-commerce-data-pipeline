{{ config(materialized='table') }}  -- what materialization for dimensions?

WITH aggregated AS (
    SELECT
        -- Primary grouping key
        zip_code_prefix,

        -- Representative coordinates per zip
        -- which aggregation function for lat/lng?
        MEDIAN(geolocation_lat) AS latitude,
        MEDIAN(geolocation_lng) AS longitude,

        -- Representative location names per zip
        -- which aggregation function for city/state?
        MODE(geolocation_city) AS city,
        MODE(geolocation_state) AS state

    FROM {{ ref('stg_geolocation') }}  -- which staging model?
    GROUP BY zip_code_prefix           -- what are you grouping by?
),

final AS (
    SELECT
        -- Surrogate key for the geolocation dimension
       {{ dbt_utils.generate_surrogate_key(['zip_code_prefix']) }} AS geo_key,

        -- All columns from aggregated
        zip_code_prefix,
        latitude,
        longitude,
        city,
        state
    FROM aggregated
)
        


SELECT * FROM final