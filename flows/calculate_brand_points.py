# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_, when

spark = (
    SparkSession.builder
    .appName("BrandPointsCurated")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .enableHiveSupport()
    .getOrCreate()
)

input_path = "hdfs://namenode:9000/data/processing/brand-event-count"
output_path = "hdfs://namenode:9000/data/curated/brand-points"

df = (
    spark.read
    .option("header", "true")
    .parquet(input_path)
    .withColumn("event_count", col("event_count").cast("long"))
)

scored_df = df.withColumn(
    "points",
    when(col("event_type") == "view", col("event_count") * 1)
    .when(col("event_type") == "cart", col("event_count") * 3)
    .when(col("event_type") == "purchase", col("event_count") * 10)
    .otherwise(0)
)

result_df = (
    scored_df
    .groupBy("brand")
    .agg(sum_("points").alias("total_points"))
)

(
    result_df.write
    .mode("overwrite")
    .option("header", "true")
    .csv(output_path)
)

spark.stop()
