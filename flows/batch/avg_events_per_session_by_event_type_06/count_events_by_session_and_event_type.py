# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "AvgEventsPerSessionByEventType"
    ).getOrCreate()


def count_events_by_session_and_event_type(data_period: str) -> None:
    processing_monthly_events_path = (
        f"hdfs://namenode:9000/data/processing/monthly_events/{data_period}"
    )
    processing_event_count_by_session_path = (
        f"hdfs://namenode:9000/data/processing/event_count_by_session/{data_period}"
    )

    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(processing_monthly_events_path)

    event_count_df = (
        df.groupBy("event_type", "user_session")
        .agg(count(col("event_type")).alias("events_count"))
        .orderBy(col("event_type").asc())
    )

    event_count_df.write.mode("overwrite").option("header", "true").csv(
        processing_event_count_by_session_path
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    count_events_by_session_and_event_type(args.data_period)
