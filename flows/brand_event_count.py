# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = (
    SparkSession.builder
    .appName("BrandEventCount")
    .config(
        "spark.hadoop.hive.metastore.uris",
        "thrift://hive-metastore:9083"
    )
    .enableHiveSupport()
    .getOrCreate()
)

df = spark.table("web_shop_db.ecommerce_events")

df = df.filter(col("brand").isNotNull())

result = (
    df.groupBy("brand", "event_type")
      .agg(count("*").alias("event_count")) 
)

(
    result.write
    .mode("overwrite")
    .option("header", "true")
    .parquet("hdfs://namenode:9000/data/processing/brand-event-count-new")

)

spark.stop()

