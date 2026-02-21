# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

DATA_PERIOD = "2020-Apr"
INPUT_FILE_NAME = f"{DATA_PERIOD}.csv"

INPUT_PATH = f"hdfs://namenode:9000/data/raw/{INPUT_FILE_NAME}"
PROCESSING_DAILY_PURCHASE_EVENTS_PATH = (
    f"hdfs://namenode:9000/data/processing/daily_purchase_events/{DATA_PERIOD}"
)


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "RankProductCategoriesByTotalRevenue-Apr2020"
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



def load_purchase_events_for_daily_metrics() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").schema(get_schema()).csv(INPUT_PATH)
    df = df.withColumn(
        "event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss z")
    )
    purchase_df = df.filter(col("event_type") == "purchase")
    purchase_df.write.mode("overwrite").option("header", "true").csv(
        PROCESSING_DAILY_PURCHASE_EVENTS_PATH
    )
    spark.stop()


if __name__ == "__main__":
    load_purchase_events_for_daily_metrics()