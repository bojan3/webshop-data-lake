CREATE TABLE webshop_customer_events_create_table (
   event_time STRING,
   event_type STRING,
   product_id STRING,
   category_id STRING, 
   category_code STRING,
   brand STRING,
   price FLOAT,
   user_id STRING,
   user_session STRING,
)
PARTITIONED BY (
    category_code STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

set hive.exec.dynamic.partition=true;                                                                           
set hive.exec.dynamic.partition.mode=nonstrict;

#create table webshop_customer_events(event_time string, event_type string, product_id string, category_id string, category_code string, brand string, price float, user_id string, user_session string);

CREATE TABLE IF NOT EXISTS webshop_customer_events1 (event_time string, event_type string, product_id string, category_id string, category_code string, brand string, price float, user_id string, user_session string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");


CREATE TABLE webshop_customer_events_create_table (
   event_time STRING,
   event_type STRING,
   product_id STRING,
   category_id STRING, 
   brand STRING,
   price FLOAT,
   user_id STRING,
   user_session STRING
)
PARTITIONED BY (category_code STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;