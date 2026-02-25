# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/webshop_data_lake"
CLICKHOUSE_TABLE = "avg_site_visits_per_user"
CLICKHOUSE_USER = "analytics"
CLICKHOUSE_PASSWORD = "analytics123"


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("AvgSiteVisitsPerUser").getOrCreate()


def publish_avg_site_visits_per_user(data_period: str) -> None:
    processing_monthly_avg_site_visits_per_user_path = (
        f"hdfs://namenode:9000/data/processing/monthly_avg_site_visits_per_user/{data_period}"
    )

    spark = create_spark_session()
    avg_site_visits_df = spark.read.option("header", "true").parquet(
        processing_monthly_avg_site_visits_per_user_path
    )

    avg_site_visits_df.write.format("jdbc").mode("append").option(
        "url", CLICKHOUSE_URL
    ).option("dbtable", CLICKHOUSE_TABLE).option(
        "driver", "com.clickhouse.jdbc.ClickHouseDriver"
    ).option("user", CLICKHOUSE_USER).option("password", CLICKHOUSE_PASSWORD).option(
        "createTableOptions", "ENGINE = MergeTree() ORDER BY batch_processing"
    ).save()
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    publish_avg_site_visits_per_user(args.data_period)
