
  create or replace   view SILVER_DB.PUBLIC.stg_customers
  
  
  
  
  as (
    

WITH source AS (
    SELECT * FROM BRONZE_DB.RAW_CUSTOMERS.raw_customers
),

cleaned AS (
    SELECT
        -- Primary key
        customer_id, --Used to connect the orders table to the customers table

        customer_unique_id, --Used to uniquely identify each of the customer

        --customer_city — should be in title case (first letter of each word capitalized)
        INITCAP(TRIM(COALESCE(customer_city,'Unknown'))) AS customer_city,
        -- customer_state — should be exactly 2 uppercase letters
        UPPER(TRIM(COALESCE(customer_state, 'XX'))) AS customer_state,

        -- customer_zip_code_prefix — should be exactly 5 digits
        LPAD(TRIM(customer_zip_code_prefix), 5, '0') AS zip_code_prefix,


        -- Audit columns
        _file_name,
        _loaded_at

    FROM source
    WHERE customer_id IS NOT NULL AND customer_unique_id IS NOT NULL
)

SELECT * FROM cleaned
  );

