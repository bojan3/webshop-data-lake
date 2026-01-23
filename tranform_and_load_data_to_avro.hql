CREATE TABLE webshop_customer_events_avro
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES ('avro.schema.literal'='{
  "namespace": "testing.hive.avro.serde",
  "name": "webshop_customer_events",
  "type": "record",
  "fields": [
    { "name":"event_time", "type":"string" },
    { "name":"event_type", "type":"string" },
    { "name":"product_id", "type":"string" },
    { "name":"category_id", "type":"string" },
    { "name":"category_code", "type":"string" },
    { "name":"brand", "type":"string" },
    { "name":"price", "type":"float" },
    { "name":"user_id", "type":"string" },
    { "name":"user_session", "type":"string" }
  ]
}');

INSERT INTO webshop_customer_events_avro
SELECT *
FROM webshop_customer_events;
