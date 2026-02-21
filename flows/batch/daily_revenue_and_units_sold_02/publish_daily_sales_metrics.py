
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date

DATA_PERIOD = "2020-Apr"

PROCESSING_DAILY_REVENUE_PATH = (
    f"hdfs://namenode:9000/data/processing/daily_revenue/{DATA_PERIOD}"
)
PROCESSING_DAILY_UNITS_SOLD_PATH = (
    f"hdfs://namenode:9000/data/processing/daily_units_sold/{DATA_PERIOD}"
)
CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/webshop_data_lake"
CLICKHOUSE_TABLE = "daily_revenue_and_units_sold_02"
CLICKHOUSE_USER = "analytics"
CLICKHOUSE_PASSWORD = "analytics123"


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "RankProductCategoriesByTotalRevenue-Apr2020"
    ).getOrCreate()


def publish_daily_sales_metrics() -> None:
    spark = create_spark_session()
    revenue_df = spark.read.option("header", "true").csv(PROCESSING_DAILY_REVENUE_PATH)
    units_sold_df = spark.read.option("header", "true").csv(
        PROCESSING_DAILY_UNITS_SOLD_PATH
    )

    published_df = (
        revenue_df.join(units_sold_df, on="event_date", how="inner")
        .select(
            to_date(col("event_date")).alias("event_date"),
            col("average_daily_revenue").cast("double").alias("average_daily_revenue"),
            col("units_sold").cast("long").alias("units_sold"),
        )
        .withColumn("batch_processing", lit(DATA_PERIOD))
        .orderBy(col("event_date").asc())
    )

    published_df.write.format("jdbc").mode("overwrite").option(
        "url", CLICKHOUSE_URL
    ).option("dbtable", CLICKHOUSE_TABLE).option(
        "driver", "com.clickhouse.jdbc.ClickHouseDriver"
    ).option("user", CLICKHOUSE_USER).option("password", CLICKHOUSE_PASSWORD).option(
        "createTableOptions", "ENGINE = MergeTree() ORDER BY event_date"
    ).save()
    spark.stop()


if __name__ == "__main__":
    publish_daily_sales_metrics()
