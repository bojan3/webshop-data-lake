# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("LongTermBrandPopularityIndex10").getOrCreate()


def count_processing_view_events_by_brand(data_period: str) -> None:
    processing_view_events_path = (
        f"hdfs://namenode:9000/data/processing/monthly_view_events/{data_period}"
    )
    processing_view_events_by_brand_count_path = (
        f"hdfs://namenode:9000/data/processing/monthly_view_events_by_brand_count/{data_period}"
    )

    spark = create_spark_session()
    view_df = spark.read.option("header", "true").csv(processing_view_events_path)

    view_count_by_brand_df = view_df.groupBy(col("brand")).agg(
        count(col("event_type")).alias("view_events_count")
    )

    view_count_by_brand_df.write.mode("overwrite").option("header", "true").csv(
        processing_view_events_by_brand_count_path
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    count_processing_view_events_by_brand(args.data_period)
