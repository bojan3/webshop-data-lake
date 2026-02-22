# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

DATA_PERIOD = "2020-Apr"

PROCESSING_VIEW_EVENTS_PATH = (
    f"hdfs://namenode:9000/data/processing/monthly_view_events/{DATA_PERIOD}"
)
PROCESSING_HOURLY_PRODUCT_VIEWS_PATH = (
    f"hdfs://namenode:9000/data/processing/hourly_product_views/{DATA_PERIOD}"
)


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("AvgHourlyProductViews-Apr2020").getOrCreate()


def count_hourly_product_views() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(PROCESSING_VIEW_EVENTS_PATH)

    hourly_views_df = (
        df.groupBy("event_date", "event_hour")
        .agg(count(col("product_id")).alias("hourly_product_views_count"))
        .orderBy(col("event_date").asc(), col("event_hour").asc())
    )

    hourly_views_df.write.mode("overwrite").option("header", "true").csv(
        PROCESSING_HOURLY_PRODUCT_VIEWS_PATH
    )
    spark.stop()


if __name__ == "__main__":
    count_hourly_product_views()
