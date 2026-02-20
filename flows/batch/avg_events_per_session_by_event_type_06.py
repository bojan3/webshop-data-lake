# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, lit, to_timestamp
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

DATA_PERIOD = "2020-Apr"
INPUT_FILE_NAME = f"{DATA_PERIOD}.csv"

INPUT_PATH = f"hdfs://namenode:9000/data/raw/{INPUT_FILE_NAME}"
PROCESSING_MONTHLY_EVENTS_PATH = (
    f"hdfs://namenode:9000/data/processing/monthly_events/{DATA_PERIOD}"
)
PROCESSING_EVENT_COUNT_BY_SESSION_PATH = (
    f"hdfs://namenode:9000/data/processing/event_count_by_session/{DATA_PERIOD}"
)
CURATED_AVG_EVENTS_PER_SESSION_PATH = (
    f"hdfs://namenode:9000/data/curated/avg_events_per_session_by_event_type/{DATA_PERIOD}"
)


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "AvgEventsPerSessionByEventType-Apr2020"
    ).getOrCreate()


def get_schema() -> StructType:
    return StructType(
        [
            StructField("event_time", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", LongType(), True),
            StructField("category_id", LongType(), True),
            StructField("category_code", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("user_id", LongType(), True),
            StructField("user_session", StringType(), True),
        ]
    )


def load_and_store_monthly_events() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").schema(get_schema()).csv(INPUT_PATH)
    events_df = df.withColumn(
        "event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss z")
    )
    events_df.write.mode("overwrite").option("header", "true").csv(
        PROCESSING_MONTHLY_EVENTS_PATH
    )
    spark.stop()


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


def calculate_average_events_per_session_by_event_type() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(PROCESSING_EVENT_COUNT_BY_SESSION_PATH)

    avg_events_df = (
        df.groupBy("event_type")
        .agg(avg(col("events_count").cast("double")).alias("avg_events_per_session"))
        .withColumn("batch_processing", lit(DATA_PERIOD))
        .orderBy(col("avg_events_per_session").desc())
    )

    avg_events_df.write.mode("overwrite").option("header", "true").csv(
        CURATED_AVG_EVENTS_PER_SESSION_PATH
    )
    spark.stop()


def main() -> None:
    load_and_store_monthly_events()
    count_events_by_session_and_event_type()
    calculate_average_events_per_session_by_event_type()


if __name__ == "__main__":
    main()
