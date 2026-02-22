# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

DATA_PERIOD = "2020-Apr"

PROCESSING_CART_EVENTS_PATH = (
    f"hdfs://namenode:9000/data/processing/monthly_cart_events/{DATA_PERIOD}"
)
PROCESSING_CART_EVENTS_BY_USER_COUNT_PATH = (
    f"hdfs://namenode:9000/data/processing/monthly_cart_events_by_user_count/{DATA_PERIOD}"
)


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "AvgMonthlyAddToCartNotPurchased-Apr2020"
    ).getOrCreate()


def count_and_store_processing_cart_events_by_user() -> None:
    spark = create_spark_session()
    cart_df = spark.read.option("header", "true").csv(PROCESSING_CART_EVENTS_PATH)

    user_event_counts_df = cart_df.groupBy(col("user_id")).agg(
        count(col("event_type")).alias("cart_event_count")
    )

    user_event_counts_df.write.mode("overwrite").option("header", "true").csv(
        PROCESSING_CART_EVENTS_BY_USER_COUNT_PATH
    )
    spark.stop()


if __name__ == "__main__":
    count_and_store_processing_cart_events_by_user()
