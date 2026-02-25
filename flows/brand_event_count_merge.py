# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_
from pyspark.sql.utils import AnalysisException

spark = (
    SparkSession.builder
    .appName("BrandEventCountUpdate")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .enableHiveSupport()
    .getOrCreate()
)

old_path = "hdfs://namenode:9000/data/processing/brand-event-count"
new_path = "hdfs://namenode:9000/data/processing/brand-event-count-new"

new_df = (
    spark.read
    .option("header", "true")
    .parquet(new_path)
    .select("brand", "event_type", "event_count")
    .withColumn("event_count", col("event_count").cast("long"))
)

try:
    old_df = (
        spark.read
        .option("header", "true")
        .parquet(old_path)
        .select("brand", "event_type", "event_count")
        .withColumn("event_count", col("event_count").cast("long"))
    )

    result_df = (
        old_df.unionByName(new_df)
        .groupBy("brand", "event_type")
        .agg(sum_("event_count").alias("event_count"))
    )

except AnalysisException:
    result_df = new_df

(
    result_df.write
    .mode("overwrite")
    .option("header", "true")
    .parquet(old_path)
)

spark.stop()
