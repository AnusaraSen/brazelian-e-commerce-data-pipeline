USE WAREHOUSE ECOMM_WH;
--Database Creation
CREATE DATABASE IF NOT EXISTS BRONZE_DB
    COMMENT = 'Raw ingested data - no transformations';

CREATE DATABASE IF NOT EXISTS SILVER_DB
    COMMENT = 'Cleaned and typed data';

CREATE DATABASE IF NOT EXISTS GOLD_DB
    COMMENT = 'Business ready analytics data';