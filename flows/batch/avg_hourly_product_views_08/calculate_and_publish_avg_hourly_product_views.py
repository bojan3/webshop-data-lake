# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, lit

DATA_PERIOD = "2020-Apr"

PROCESSING_HOURLY_PRODUCT_VIEWS_PATH = (
    f"hdfs://namenode:9000/data/processing/hourly_product_views/{DATA_PERIOD}"
)
CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/webshop_data_lake"
CLICKHOUSE_TABLE = "avg_hourly_product_views_08"
CLICKHOUSE_USER = "analytics"
CLICKHOUSE_PASSWORD = "analytics123"


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("AvgHourlyProductViews-Apr2020").getOrCreate()


def calculate_and_publish_avg_hourly_product_views() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(PROCESSING_HOURLY_PRODUCT_VIEWS_PATH)

    avg_hourly_views_df = (
        df.groupBy("event_hour")
        .agg(
            avg(col("hourly_product_views_count").cast("double")).alias(
                "avg_hourly_product_views"
            )
        )
        .withColumn("event_hour", col("event_hour").cast("int"))
        .withColumn("batch_processing", lit(DATA_PERIOD))
        .select("event_hour", "avg_hourly_product_views", "batch_processing")
        .orderBy(col("event_hour").asc())
    )

    avg_hourly_views_df.write.format("jdbc").mode("overwrite").option(
        "url", CLICKHOUSE_URL
    ).option("dbtable", CLICKHOUSE_TABLE).option(
        "driver", "com.clickhouse.jdbc.ClickHouseDriver"
    ).option("user", CLICKHOUSE_USER).option("password", CLICKHOUSE_PASSWORD).option(
        "createTableOptions", "ENGINE = MergeTree() ORDER BY event_hour"
    ).save()
    spark.stop()


if __name__ == "__main__":
    calculate_and_publish_avg_hourly_product_views()
