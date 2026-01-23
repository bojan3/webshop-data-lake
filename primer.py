#!/usr/bin/python
### before spark-submit: export PYTHONIOENCODING=utf8

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf().setAppName("uni").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

quiet_logs(spark)

from pyspark.sql.types import *

schemaString = "event_time event_type product_id category_id category_code brand price user_id user_session"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

df = spark.read.csv("hdfs://namenode:9000/user/hive/warehouse/webshop_customer_events/events.csv", header=True, mode="DROPMALFORMED", schema=schema)

add_to_cart_num = df.filter(df.event_type == "cart").count()
purchase_num = df.filter(df.event_type == "purchase").count()
cart_conversion_rate =  (purchase_num / add_to_cart_num) * 100

print(f"add_to_cart_num: {add_to_cart_num}")
print(f"purchase_num: {purchase_num}")
print(f"cart_conversion_rate: {cart_conversion_rate}")