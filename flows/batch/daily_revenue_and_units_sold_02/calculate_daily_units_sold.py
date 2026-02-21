# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, to_date

DATA_PERIOD = "2020-Apr"
INPUT_FILE_NAME = f"{DATA_PERIOD}.csv"

PROCESSING_DAILY_PURCHASE_EVENTS_PATH = (
    f"hdfs://namenode:9000/data/processing/daily_purchase_events/{DATA_PERIOD}"
)
PROCESSING_DAILY_UNITS_SOLD_PATH = (
    f"hdfs://namenode:9000/data/processing/daily_units_sold/{DATA_PERIOD}"
)


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "RankProductCategoriesByTotalRevenue-Apr2020"
    ).getOrCreate()


def calculate_daily_units_sold() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(PROCESSING_DAILY_PURCHASE_EVENTS_PATH)

    daily_units_sold_df = (
        df.withColumn("event_date", to_date(col("event_time")))
        .groupBy("event_date")
        .agg(count(col("product_id")).alias("units_sold"))
        .orderBy(col("event_date").asc())
    )

    daily_units_sold_df.write.mode("overwrite").option("header", "true").csv(
        PROCESSING_DAILY_UNITS_SOLD_PATH
    )
    spark.stop()


if __name__ == "__main__":
    calculate_daily_units_sold()