# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_date, to_timestamp
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("AvgHourlyProductViews").getOrCreate()


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


def load_and_store_processing_view_events(data_period: str) -> None:
    input_file_name = f"{data_period}.csv"
    input_path = f"hdfs://namenode:9000/data/raw/{input_file_name}"
    processing_view_events_path = (
        f"hdfs://namenode:9000/data/processing/monthly_view_events/{data_period}"
    )

    spark = create_spark_session()
    df = spark.read.option("header", "true").schema(get_schema()).csv(input_path)
    view_df = (
        df.withColumn(
            "event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss z")
        )
        .withColumn("event_date", to_date(col("event_time")))
        .withColumn("event_hour", hour(col("event_time")))
        .filter(col("event_type") == "view")
        .select("event_date", "event_hour", "event_type", "product_id", "user_id")
    )
    view_df.write.mode("overwrite").option("header", "true").csv(
        processing_view_events_path
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    load_and_store_processing_view_events(args.data_period)
