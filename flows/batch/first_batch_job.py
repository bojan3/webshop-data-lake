# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

spark = SparkSession.builder.appName("EventsBatchJob-Apr2020").getOrCreate()

schema = StructType(
    [
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("category_id", LongType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True),
    ]
)

df = (
    spark.read.option("header", "true")
    .schema(schema)
    .csv("hdfs://namenode:9000/data/raw/events/2020-Apr.csv")
)

df = df.withColumn(
    "event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss z")
)

result = (
    df.filter(col("event_type") == "view")
    .groupBy("brand")
    .count()
    .orderBy(col("count").desc())
)

result.show(20)

result.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/data/processed/views_by_brand/2020-04"
)

spark.stop()
