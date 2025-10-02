/* 
### GROUP 6 ###
- Anna Mika

Our data pipieline could be used primarily for inventory optimization and sales forecasting.

Our data will help us understand 1) which products sell the fastest, 2) which products are underperforming, 3) how seasonality affects demand, and 4) how much stock to reorder in order to avoiding waste and stockouts. 

Moving forward, our pipeline can be used to generate a weekly report (or a dashboard) showing which items are running low based on sales velocity. This prevents overproduction (important for sustainability) and avoids stockouts.
We could also implement seasonal insights, ex.: Identify that "Cold Brew Blend" spikes in summer, while "Golden Turmeric Latte Mix" sells better in winter. These insights would help inform marketing campaigns and warehouse prep.
The report can also be important for identifying underperformers and deciding whether to discontinue or repackage the product.
And finally, because we have decided to be an eco-friendly brand, the pipeline helps forecast demand accurately which results in less unsold coffee, fewer expired products, less generated waste, and more efficient sourcing.

*/

USE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS INGEST;
CREATE ROLE IF NOT EXISTS INGEST;
GRANT USAGE ON WAREHOUSE INGEST TO ROLE INGEST;
GRANT OPERATE ON WAREHOUSE INGEST TO ROLE INGEST;

CREATE DATABASE IF NOT EXISTS INGEST;
USE DATABASE INGEST;
CREATE SCHEMA IF NOT EXISTS INGEST;
USE SCHEMA INGEST;

GRANT OWNERSHIP ON DATABASE INGEST TO ROLE INGEST;
GRANT OWNERSHIP ON SCHEMA INGEST.INGEST TO ROLE INGEST;

CREATE USER INGEST PASSWORD='ETL_WAREHOUSING' LOGIN_NAME='INGEST' 
    MUST_CHANGE_PASSWORD=FALSE, DISABLED=FALSE, 
    DEFAULT_WAREHOUSE='INGEST', DEFAULT_NAMESPACE='INGEST.INGEST', 
    DEFAULT_ROLE='INGEST';

GRANT ROLE INGEST TO USER INGEST;

SET USERNAME=CURRENT_USER();
GRANT ROLE INGEST TO USER IDENTIFIER($USERNAME);


USE ROLE INGEST;

CREATE OR REPLACE TABLE CLIENT_SUPPORT_ORDERS (
    TXID VARCHAR(255) NOT NULL,
    RFID VARCHAR(255) NOT NULL,  
    ITEM VARCHAR(255) NOT NULL,
    PURCHASE_TIME TIMESTAMP NOT NULL,
    EXPIRATION_TIME DATE NOT NULL,
    DAYS NUMBER NOT NULL,
    NAME VARCHAR(255) NOT NULL,
    ADDRESS VARIANT,                     
    PHONE VARCHAR(255),                  
    EMAIL VARCHAR(255),                  
    EMERGENCY_CONTACT VARIANT,           
    PRIMARY KEY (TXID)
);


COMMENT ON TABLE CLIENT_SUPPORT_ORDERS IS 'Customer orders for Faire Trade Coffee Co inventory';
COMMENT ON COLUMN CLIENT_SUPPORT_ORDERS.ITEM IS 'Product from Faire Trade Coffee Co inventory';
COMMENT ON COLUMN CLIENT_SUPPORT_ORDERS.ADDRESS IS 'JSON: {street_address, city, state, postalcode} or NULL';
COMMENT ON COLUMN CLIENT_SUPPORT_ORDERS.EMERGENCY_CONTACT IS 'JSON: {name, phone} or NULL';


ALTER USER INGEST SET RSA_PUBLIC_KEY='-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAmSj8BH8SCQfVVNPG/yAK
/maTxPBdmpP1z7Czw6W1tbP2xbKxUr6MXjznsXlLnvXbJvgSzxyDnTQFrl/3NKcQ
iOKzwKbIVHcZva1+H/FdZWnfO2rhetaOn8k5xZDc68pBapHdxmgItQ5qc3U4aRnZ
QsUhyT2a6+U5weXc0GXX/4uFLvNaSuoeopXyLZmWqW9nsWFYGBfyX+HFDV+FtLQ7
djtJNlefSaW4XtuLr62zNJYJyVjJRN9ElWZ8djb08Mhb+tf8TObetKaP193bKL1b
w/dczfCXKx5Os/BiHqbITMlhPkyVYnmlVqY+K1hvx2ght6r+9hYcLeLc+R4Q0wtt
noC4lrBNq9dzOQI9OrEfbNxT/f8fmSm8hl9gqUzlRlH6JLvrsQUrjJq/XNrFyk8U
nKS0zqFscQiA9PhdnpcBfQy9c1KIFlhu7xCx0HrjoGampzuEC+nK4sfP8ED7mFJ4
a1A7JAWpmx2dn/TZjqSpkmIfZSAQBi3jrXedm4TUjP/t7a8RSGVDAgljwq4UDzuy
e6DYuUntiaAEcQNsLjuzW8nfA8ElfQXnQjB75RH+S00FZeauD1g8QUD/dcEX9Cot
wu4uU/6W3+pgXBoKV/4kL4I5HSgY66y7aBCQuwJUlqjxIalXkVq2yC5HKnFHTFan
DSNsthJdNcWN+FH6t4sa7bUCAwEAAQ==
-----END PUBLIC KEY-----';


USE ROLE INGEST;
USE DATABASE INGEST;
USE SCHEMA INGEST;

SELECT COUNT(*) FROM CLIENT_SUPPORT_ORDERS;

SELECT * FROM CLIENT_SUPPORT_ORDERS LIMIT 5;

SELECT ITEM, COUNT(*) as order_count 
FROM CLIENT_SUPPORT_ORDERS 
GROUP BY ITEM 
ORDER BY order_count DESC;
