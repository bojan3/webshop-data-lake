# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, to_timestamp
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("RankElectronicsBrandsByUnitsSold").getOrCreate()


def get_schema() -> StructType:
    return StructType(
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


def load_and_store_electronics_purchase_events(data_period: str) -> None:
    input_file_name = f"{data_period}.csv"
    input_path = f"hdfs://namenode:9000/data/raw/{input_file_name}"
    processing_electronics_purchases_path = (
        f"hdfs://namenode:9000/data/processing/electronics_brand_purchases/{data_period}"
    )

    spark = create_spark_session()
    df = spark.read.option("header", "true").schema(get_schema()).csv(input_path)
    purchase_df = (
        df.withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss z"))
        .filter(col("event_type") == "purchase")
        .filter(col("brand").isNotNull())
        .filter(col("category_code").isNotNull())
        .filter(lower(col("category_code")).startswith("electronics"))
    )

    purchase_df.write.mode("overwrite").option("header", "true").parquet(
        processing_electronics_purchases_path
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    load_and_store_electronics_purchase_events(args.data_period)
