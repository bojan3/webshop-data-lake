# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum

PROCESSING_LONG_TERM_BRAND_POPULARITY_INDEX_BASE_PATH = (
    "hdfs://namenode:9000/data/processing/monthly_long_term_brand_popularity_index/"
)
PROCESSING_TOTAL_LONG_TERM_BRAND_POPULARITY_INDEX_BASE_PATH = (
    "hdfs://namenode:9000/data/processing/total_long_term_brand_popularity_index/"
)

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/webshop_data_lake"
CLICKHOUSE_TABLE = "long_term_brand_popularity_index_total"
CLICKHOUSE_USER = "analytics"
CLICKHOUSE_PASSWORD = "analytics123"


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("LongTermBrandPopularityIndexTotal").getOrCreate()


def sum_all_previous_months_and_publish_long_term_brand_popularity_index(data_period: str) -> None:
    spark = create_spark_session()
    processing_total_long_term_brand_popularity_index_path = (
        f"{PROCESSING_TOTAL_LONG_TERM_BRAND_POPULARITY_INDEX_BASE_PATH}{data_period}"
    )
    df = (
        spark.read.option("header", "true")
        .option("recursiveFileLookup", "true")
        .parquet(PROCESSING_LONG_TERM_BRAND_POPULARITY_INDEX_BASE_PATH)
    )

    total_index_df = (
        df.groupBy("brand")
        .agg(
            sum(col("long_term_brand_popularity_index").cast("long")).alias(
                "total_long_term_brand_popularity_index"
            )
        )
        .withColumn("batch_processing", lit(data_period))
        .orderBy(col("total_long_term_brand_popularity_index").desc())
    )

    total_index_df.write.mode("overwrite").option("header", "true").parquet(
        processing_total_long_term_brand_popularity_index_path
    )

    total_index_df.write.format("jdbc").mode("overwrite").option(
        "url", CLICKHOUSE_URL
    ).option("dbtable", CLICKHOUSE_TABLE).option(
        "driver", "com.clickhouse.jdbc.ClickHouseDriver"
    ).option("user", CLICKHOUSE_USER).option("password", CLICKHOUSE_PASSWORD).option(
        "createTableOptions", "ENGINE = MergeTree() ORDER BY brand"
    ).save()
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    sum_all_previous_months_and_publish_long_term_brand_popularity_index(
        args.data_period
    )
