# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("AvgHourlyProductViews").getOrCreate()


def count_hourly_product_views(data_period: str) -> None:
    processing_view_events_path = (
        f"hdfs://namenode:9000/data/processing/monthly_view_events/{data_period}"
    )
    processing_hourly_product_views_path = (
        f"hdfs://namenode:9000/data/processing/hourly_product_views/{data_period}"
    )

    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(processing_view_events_path)

    hourly_views_df = (
        df.groupBy("event_date", "event_hour")
        .agg(count(col("product_id")).alias("hourly_product_views_count"))
        .orderBy(col("event_date").asc(), col("event_hour").asc())
    )

    hourly_views_df.write.mode("overwrite").option("header", "true").csv(
        processing_hourly_product_views_path
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    count_hourly_product_views(args.data_period)
