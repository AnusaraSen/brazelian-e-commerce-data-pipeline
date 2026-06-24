

WITH source AS (
    SELECT * FROM BRONZE_DB.RAW_ORDERS.raw_orders
),

cleaned AS (
    SELECT
        -- Primary key
        order_id,

        -- Foreign keys
        customer_id,

        -- Order status
        LOWER(TRIM(order_status))           AS order_status,

        -- Timestamps cast from VARCHAR to proper types
        TRY_TO_TIMESTAMP(order_purchase_timestamp)        
            AS order_purchase_timestamp,
        TRY_TO_TIMESTAMP(order_approved_at)               
            AS order_approved_at,
        TRY_TO_TIMESTAMP(order_delivered_carrier_date)    
            AS order_delivered_carrier_date,
        TRY_TO_TIMESTAMP(order_delivered_customer_date)   
            AS order_delivered_customer_date,
        TRY_TO_TIMESTAMP(order_estimated_delivery_date)   
            AS order_estimated_delivery_date,

        -- Derived columns
        DATEDIFF(
            'day',
            TRY_TO_TIMESTAMP(order_purchase_timestamp),
            TRY_TO_TIMESTAMP(order_delivered_customer_date)
        )                                   AS days_to_deliver,

        CASE
            WHEN TRY_TO_TIMESTAMP(order_delivered_customer_date)
                 <= TRY_TO_TIMESTAMP(order_estimated_delivery_date)
            THEN TRUE
            ELSE FALSE
        END                                 AS delivered_on_time,

        -- Audit columns
        _file_name,
        _loaded_at

    FROM source
    WHERE order_id IS NOT NULL
)

SELECT * FROM cleaned