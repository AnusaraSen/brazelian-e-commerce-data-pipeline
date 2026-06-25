

WITH source AS (
    SELECT * FROM BRONZE_DB.RAW_ORDER_ITEMS.raw_order_items
),

cleaned AS (
    SELECT
       
       --primary key
       order_id,

       --Type cast from VARCHAR to DECIMAL 
       TRY_TO_DECIMAL(order_item) AS order_item_id,

       --foreign keys 
       product_id,
       seller_id,

       -- Type cast from VARCHAR to TIMESTAMP
       TRY_TO_TIMESTAMP(shipping_limit_date) AS shipping_limit_date,

       -- Type cast from VARCHAR to DECIMAL
       TRY_TO_DECIMAL(price, 10, 2) AS price, 

       -- Type cast from VARCHAR to DECIMAL
       TRY_TO_DECIMAL(freight_value, 10, 2) AS freight_value,
       
       --Derived column: order_item_total = price + freight_value
       TRY_TO_DECIMAL(price, 10, 2) + 
       TRY_TO_DECIMAL(freight_value, 10, 2) AS order_item_total,

        -- Audit columns
        _file_name,
        _loaded_at

    FROM source
    WHERE product_id IS NOT NULL AND order_id IS NOT NULL AND seller_id IS NOT NULL AND order_item_id IS NOT NULL
)

SELECT * FROM cleaned