# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/webshop_data_lake"
CLICKHOUSE_TABLE = "daily_revenue_and_units_sold_02"
CLICKHOUSE_USER = "analytics"
CLICKHOUSE_PASSWORD = "analytics123"


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "RankProductCategoriesByTotalRevenue"
    ).getOrCreate()


def publish_daily_sales_metrics(data_period: str) -> None:
    processing_daily_revenue_path = (
        f"hdfs://namenode:9000/data/processing/daily_revenue/{data_period}"
    )
    processing_daily_units_sold_path = (
        f"hdfs://namenode:9000/data/processing/daily_units_sold/{data_period}"
    )

    spark = create_spark_session()
    revenue_df = spark.read.option("header", "true").csv(processing_daily_revenue_path)
    units_sold_df = spark.read.option("header", "true").csv(
        processing_daily_units_sold_path
    )

    published_df = (
        revenue_df.join(units_sold_df, on="event_date", how="inner")
        .select(
            to_date(col("event_date")).alias("event_date"),
            col("average_daily_revenue").cast("double").alias("average_daily_revenue"),
            col("units_sold").cast("long").alias("units_sold"),
        )
        .withColumn("batch_processing", lit(data_period))
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    publish_daily_sales_metrics(args.data_period)
