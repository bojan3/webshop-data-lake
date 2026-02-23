# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, to_date


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "RankProductCategoriesByTotalRevenue"
    ).getOrCreate()


def calculate_daily_units_sold(data_period: str) -> None:
    processing_daily_purchase_events_path = (
        f"hdfs://namenode:9000/data/processing/daily_purchase_events/{data_period}"
    )
    processing_daily_units_sold_path = (
        f"hdfs://namenode:9000/data/processing/daily_units_sold/{data_period}"
    )

    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(processing_daily_purchase_events_path)

    daily_units_sold_df = (
        df.withColumn("event_date", to_date(col("event_time")))
        .groupBy("event_date")
        .agg(count(col("product_id")).alias("units_sold"))
        .orderBy(col("event_date").asc())
    )

    daily_units_sold_df.write.mode("overwrite").option("header", "true").csv(
        processing_daily_units_sold_path
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    calculate_daily_units_sold(args.data_period)
