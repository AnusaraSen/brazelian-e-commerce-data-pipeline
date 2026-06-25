

WITH source AS (
    SELECT * FROM BRONZE_DB.RAW_ORDER_PAYMENTS.raw_order_payments
),

cleaned AS (
    SELECT
        --Foreign key
        order_id,
        --Type cast from VARCHAR to DECIMAL
        TRY_TO_DECIMAL(payment_sequence) AS payment_sequential,

        CASE
            WHEN LOWER(TRIM(payment_type)) = 'not_defined' 
            THEN 'Unknown'
            ELSE INITCAP(TRIM(payment_type))
        END AS payment_type,

        TRY_TO_DECIMAL(payment_installments) AS payment_installments,
        TRY_TO_DECIMAL(payment_value, 10, 2) AS payment_value,
        
        -- Audit columns
        _file_name,
        _loaded_at


    FROM source
    WHERE order_id IS NOT NULL AND  payment_value IS NOT NULL 
)

SELECT * FROM cleaned