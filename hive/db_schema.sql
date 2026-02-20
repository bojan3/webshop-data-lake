CREATE DATABASE web_shop_db;

CREATE EXTERNAL TABLE ecommerce_events (
    event_time TIMESTAMP,
    event_type STRING,
    product_id INT,
    category_id BIGINT,
    category_code STRING,
    brand STRING,
    price DECIMAL(10, 2),
    user_id BIGINT,
    user_session STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw'
TBLPROPERTIES ("skip.header.line.count"="1");