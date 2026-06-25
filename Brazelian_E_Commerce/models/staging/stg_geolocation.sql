{{config(materialized='view')}}

WITH source AS (
    SELECT * FROM {{ source('bronze_geolocation', 'raw_geolocations') }}
),

cleaned AS (
    SELECT
        
        LPAD(TRIM(geolocation_zip_code_prefix), 5, '0') AS zip_code_prefix,

        TRY_TO_DOUBLE(geolocation_lat) AS geolocation_lat,
        TRY_TO_DOUBLE(geolocation_lng) AS geolocation_lng,

        INITCAP(TRIM(COALESCE(geolocation_city,'Unknown'))) AS geolocation_city,

        -- geolocation_state — should be exactly 2 uppercase letters
        UPPER(TRIM(COALESCE(geolocation_state, 'XX'))) AS geolocation_state,
        
        

        -- Audit columns
        _file_name,
        _loaded_at

    FROM source
    WHERE  geolocation_zip_code_prefix IS NOT NULL
)

SELECT * FROM cleaned