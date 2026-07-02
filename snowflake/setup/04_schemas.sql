USE DATABASE BRONZE_DB;

CREATE SCHEMA IF NOT EXISTS RAW_ORDERS
    COMMENT = 'Olist raw order data';
    
CREATE SCHEMA IF NOT EXISTS RAW_PRODUCTS
    COMMENT = 'Olist raw product data';
    
CREATE SCHEMA IF NOT EXISTS RAW_SELLERS
    COMMENT = 'Olist raw sellers data';
    
CREATE SCHEMA IF NOT EXISTS RAW_PRODUCT_CATEGORY_NAME_TRANSLATION
    COMMENT = 'Olist raw product category name translation data';
    
CREATE SCHEMA IF NOT EXISTS RAW_CUSTOMERS
    COMMENT = 'Olist raw customers data';
    
CREATE SCHEMA IF NOT EXISTS RAW_GEOLOCATIONS
    COMMENT = 'Olist raw geolocation data';
    
CREATE SCHEMA IF NOT EXISTS RAW_ORDER_ITEMS
    COMMENT = 'Olist raw order item data';
    
CREATE SCHEMA IF NOT EXISTS RAW_ORDER_PAYMENTS
    COMMENT = 'Olist raw payment data';
    
CREATE SCHEMA IF NOT EXISTS RAW_ORDER_REVIEWS
    COMMENT = 'Olist raw order data';