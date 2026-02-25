# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, lit

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/webshop_data_lake"
CLICKHOUSE_TABLE = "avg_cart_purchase_count_diff"
CLICKHOUSE_USER = "analytics"
CLICKHOUSE_PASSWORD = "analytics123"


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "AvgMonthlyAddToCartNotPurchased"
    ).getOrCreate()


def publish_avg_cart_purchase_count_diff(data_period: str) -> None:
    processing_cart_purchase_count_diff_path = (
        f"hdfs://namenode:9000/data/processing/monthly_cart_purchase_count_diff/{data_period}"
    )

    spark = create_spark_session()
    df = spark.read.option("header", "true").parquet(processing_cart_purchase_count_diff_path)

    avg_diff_df = (
        df.agg(
            avg(col("cart_purchase_count_diff").cast("double")).alias(
                "avg_cart_purchase_count_diff"
            )
        )
        .withColumn("batch_processing", lit(data_period))
        .select("avg_cart_purchase_count_diff", "batch_processing")
    )

    avg_diff_df.write.format("jdbc").mode("overwrite").option(
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
    publish_avg_cart_purchase_count_diff(args.data_period)
