# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, to_date


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "RankProductCategoriesByTotalRevenue"
    ).getOrCreate()


def calculate_daily_revenue(data_period: str) -> None:
    processing_daily_purchase_events_path = (
        f"hdfs://namenode:9000/data/processing/daily_purchase_events/{data_period}"
    )
    processing_daily_revenue_path = (
        f"hdfs://namenode:9000/data/processing/daily_revenue/{data_period}"
    )

    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(processing_daily_purchase_events_path)

    daily_revenue_df = (
        df.withColumn("event_date", to_date(col("event_time")))
        .groupBy("event_date")
        .agg(avg(col("price").cast("double")).alias("average_daily_revenue"))
        .orderBy(col("event_date").asc())
    )

    daily_revenue_df.write.mode("overwrite").option("header", "true").csv(
        processing_daily_revenue_path
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    calculate_daily_revenue(args.data_period)
