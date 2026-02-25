# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, lit


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "AvgMonthlyAddToCartNotPurchased"
    ).getOrCreate()


def join_cart_and_purchase_counts(data_period: str) -> None:
    processing_cart_events_by_user_count_path = (
        f"hdfs://namenode:9000/data/processing/monthly_cart_events_by_user_count/{data_period}"
    )
    processing_purchase_events_by_user_count_path = (
        f"hdfs://namenode:9000/data/processing/monthly_purchase_events_by_user_count/{data_period}"
    )
    processing_cart_purchase_count_diff_path = (
        f"hdfs://namenode:9000/data/processing/monthly_cart_purchase_count_diff/{data_period}"
    )

    spark = create_spark_session()

    cart_counts_df = spark.read.option("header", "true").parquet(
        processing_cart_events_by_user_count_path
    )
    purchase_counts_df = spark.read.option("header", "true").parquet(
        processing_purchase_events_by_user_count_path
    )

    result_df = (
        cart_counts_df.alias("cart")
        .join(purchase_counts_df.alias("purchase"), on="user_id", how="left")
        .select(
            col("user_id"),
            (
                col("cart.cart_event_count").cast("long")
                - coalesce(col("purchase.purchase_event_count").cast("long"), lit(0))
            ).alias("cart_purchase_count_diff")
        )
    )

    result_df.write.mode("overwrite").option("header", "true").parquet(
        processing_cart_purchase_count_diff_path
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    join_cart_and_purchase_counts(args.data_period)
