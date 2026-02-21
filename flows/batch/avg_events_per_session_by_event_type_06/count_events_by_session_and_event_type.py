# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

DATA_PERIOD = "2020-Apr"

PROCESSING_MONTHLY_EVENTS_PATH = (
    f"hdfs://namenode:9000/data/processing/monthly_events/{DATA_PERIOD}"
)
PROCESSING_EVENT_COUNT_BY_SESSION_PATH = (
    f"hdfs://namenode:9000/data/processing/event_count_by_session/{DATA_PERIOD}"
)


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "AvgEventsPerSessionByEventType-Apr2020"
    ).getOrCreate()


def count_events_by_session_and_event_type() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(PROCESSING_MONTHLY_EVENTS_PATH)

    event_count_df = (
        df.groupBy("event_type", "user_session")
        .agg(count(col("event_type")).alias("events_count"))
        .orderBy(col("event_type").asc())
    )

    event_count_df.write.mode("overwrite").option("header", "true").csv(
        PROCESSING_EVENT_COUNT_BY_SESSION_PATH
    )
    spark.stop()


if __name__ == "__main__":
    count_events_by_session_and_event_type()

