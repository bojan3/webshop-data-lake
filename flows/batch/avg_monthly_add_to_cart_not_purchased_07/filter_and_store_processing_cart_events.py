# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "AvgMonthlyAddToCartNotPurchased"
    ).getOrCreate()


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


def filter_and_store_processing_cart_events(data_period: str) -> None:
    input_file_name = f"{data_period}.csv"
    input_path = f"hdfs://namenode:9000/data/raw/{input_file_name}"
    processing_cart_events_path = (
        f"hdfs://namenode:9000/data/processing/monthly_cart_events/{data_period}"
    )

    spark = create_spark_session()
    df = spark.read.option("header", "true").schema(get_schema()).csv(input_path)
    cart_df = df.filter(col("event_type") == "cart").select(
        "event_time", "user_id", "event_type", "product_id"
    )
    cart_df.write.mode("overwrite").option("header", "true").csv(
        processing_cart_events_path
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    filter_and_store_processing_cart_events(args.data_period)
