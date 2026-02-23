# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "AvgMonthlyAddToCartNotPurchased"
    ).getOrCreate()


def count_and_store_processing_cart_events_by_user(data_period: str) -> None:
    processing_cart_events_path = (
        f"hdfs://namenode:9000/data/processing/monthly_cart_events/{data_period}"
    )
    processing_cart_events_by_user_count_path = (
        f"hdfs://namenode:9000/data/processing/monthly_cart_events_by_user_count/{data_period}"
    )

    spark = create_spark_session()
    cart_df = spark.read.option("header", "true").csv(processing_cart_events_path)

    user_event_counts_df = cart_df.groupBy(col("user_id")).agg(
        count(col("event_type")).alias("cart_event_count")
    )

    user_event_counts_df.write.mode("overwrite").option("header", "true").csv(
        processing_cart_events_by_user_count_path
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    count_and_store_processing_cart_events_by_user(args.data_period)
