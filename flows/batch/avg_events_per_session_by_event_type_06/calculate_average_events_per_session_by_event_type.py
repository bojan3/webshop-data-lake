# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, lit

DATA_PERIOD = "2020-Apr"

PROCESSING_EVENT_COUNT_BY_SESSION_PATH = (
    f"hdfs://namenode:9000/data/processing/event_count_by_session/{DATA_PERIOD}"
)
CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/webshop_data_lake"
CLICKHOUSE_TABLE = "avg_events_per_session_by_event_type"
CLICKHOUSE_USER = "analytics"
CLICKHOUSE_PASSWORD = "analytics123"


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "AvgEventsPerSessionByEventType-Apr2020"
    ).getOrCreate()


def calculate_average_events_per_session_by_event_type() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(PROCESSING_EVENT_COUNT_BY_SESSION_PATH)

    avg_events_df = (
        df.groupBy("event_type")
        .agg(avg(col("events_count").cast("double")).alias("avg_events_per_session"))
        .withColumn("batch_processing", lit(DATA_PERIOD))
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


if __name__ == "__main__":
    calculate_average_events_per_session_by_event_type()
