# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, lit

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/webshop_data_lake"
CLICKHOUSE_TABLE = "avg_hourly_product_views_08"
CLICKHOUSE_USER = "analytics"
CLICKHOUSE_PASSWORD = "analytics123"


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("AvgHourlyProductViews").getOrCreate()


def calculate_and_publish_avg_hourly_product_views(data_period: str) -> None:
    processing_hourly_product_views_path = (
        f"hdfs://namenode:9000/data/processing/hourly_product_views/{data_period}"
    )

    spark = create_spark_session()
    df = spark.read.option("header", "true").parquet(processing_hourly_product_views_path)

    avg_hourly_views_df = (
        df.groupBy("event_hour")
        .agg(
            avg(col("hourly_product_views_count").cast("double")).alias(
                "avg_hourly_product_views"
            )
        )
        .withColumn("event_hour", col("event_hour").cast("int"))
        .withColumn("batch_processing", lit(data_period))
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    calculate_and_publish_avg_hourly_product_views(args.data_period)
