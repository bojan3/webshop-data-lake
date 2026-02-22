# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, lit

DATA_PERIOD = "2020-Apr"

PROCESSING_CART_EVENTS_BY_USER_COUNT_PATH = (
    f"hdfs://namenode:9000/data/processing/monthly_cart_events_by_user_count/{DATA_PERIOD}"
)
PROCESSING_PURCHASE_EVENTS_BY_USER_COUNT_PATH = (
    f"hdfs://namenode:9000/data/processing/monthly_purchase_events_by_user_count/{DATA_PERIOD}"
)
PROCESSING_CART_PURCHASE_COUNT_DIFF_PATH = (
    f"hdfs://namenode:9000/data/processing/monthly_cart_purchase_count_diff/{DATA_PERIOD}"
)


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "AvgMonthlyAddToCartNotPurchased-Apr2020"
    ).getOrCreate()


def join_cart_and_purchase_counts() -> None:
    spark = create_spark_session()

    cart_counts_df = spark.read.option("header", "true").csv(
        PROCESSING_CART_EVENTS_BY_USER_COUNT_PATH
    )
    purchase_counts_df = spark.read.option("header", "true").csv(
        PROCESSING_PURCHASE_EVENTS_BY_USER_COUNT_PATH
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

    result_df.write.mode("overwrite").option("header", "true").csv(
        PROCESSING_CART_PURCHASE_COUNT_DIFF_PATH
    )
    spark.stop()


if __name__ == "__main__":
    join_cart_and_purchase_counts()
