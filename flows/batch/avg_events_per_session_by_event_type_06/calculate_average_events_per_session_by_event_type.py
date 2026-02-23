# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, lit

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/webshop_data_lake"
CLICKHOUSE_TABLE = "avg_events_per_session_by_event_type"
CLICKHOUSE_USER = "analytics"
CLICKHOUSE_PASSWORD = "analytics123"


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "AvgEventsPerSessionByEventType"
    ).getOrCreate()


def calculate_average_events_per_session_by_event_type(data_period: str) -> None:
    processing_event_count_by_session_path = (
        f"hdfs://namenode:9000/data/processing/event_count_by_session/{data_period}"
    )

    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(processing_event_count_by_session_path)

    avg_events_df = (
        df.groupBy("event_type")
        .agg(avg(col("events_count").cast("double")).alias("avg_events_per_session"))
        .withColumn("batch_processing", lit(data_period))
        .orderBy(col("avg_events_per_session").desc())
    )

    avg_events_df.write.format("jdbc").mode("overwrite").option(
        "url", CLICKHOUSE_URL
    ).option("dbtable", CLICKHOUSE_TABLE).option(
        "driver", "com.clickhouse.jdbc.ClickHouseDriver"
    ).option("user", CLICKHOUSE_USER).option("password", CLICKHOUSE_PASSWORD).option(
        "createTableOptions", "ENGINE = MergeTree() ORDER BY event_type"
    ).save()
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    calculate_average_events_per_session_by_event_type(args.data_period)
