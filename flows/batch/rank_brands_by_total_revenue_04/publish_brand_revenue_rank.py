# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/webshop_data_lake"
CLICKHOUSE_TABLE = "brand_revenue_rank"
CLICKHOUSE_USER = "analytics"
CLICKHOUSE_PASSWORD = "analytics123"


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("RankBrandsByTotalRevenue").getOrCreate()


def publish_brand_revenue_rank(data_period: str) -> None:
    processing_brand_revenue_sum_path = (
        f"hdfs://namenode:9000/data/processing/brand_revenue_sum/{data_period}"
    )

    spark = create_spark_session()
    df = spark.read.option("header", "true").parquet(processing_brand_revenue_sum_path)

    ranked_df = (
        df.select(
            "rank_brand_name",
            col("total_revenue").cast("double").alias("total_revenue"),
        )
        .withColumn("batch_processing", lit(data_period))
        .orderBy(col("total_revenue").desc())
    )

    ranked_df.write.format("jdbc").mode("overwrite").option(
        "url", CLICKHOUSE_URL
    ).option("dbtable", CLICKHOUSE_TABLE).option(
        "driver", "com.clickhouse.jdbc.ClickHouseDriver"
    ).option("user", CLICKHOUSE_USER).option("password", CLICKHOUSE_PASSWORD).option(
        "createTableOptions", "ENGINE = MergeTree() ORDER BY rank_brand_name"
    ).save()
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    publish_brand_revenue_rank(args.data_period)
