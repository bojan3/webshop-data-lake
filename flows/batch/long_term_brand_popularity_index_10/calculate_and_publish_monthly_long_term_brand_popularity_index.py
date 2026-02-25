# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, lit

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/webshop_data_lake"
CLICKHOUSE_TABLE = "long_term_brand_popularity_index_10"
CLICKHOUSE_USER = "analytics"
CLICKHOUSE_PASSWORD = "analytics123"


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("LongTermBrandPopularityIndex10").getOrCreate()


def calculate_and_publish_monthly_long_term_brand_popularity_index(data_period: str) -> None:
    processing_cart_events_by_brand_count_path = (
        f"hdfs://namenode:9000/data/processing/monthly_cart_events_by_brand_count/{data_period}"
    )
    processing_purchase_events_by_brand_count_path = (
        f"hdfs://namenode:9000/data/processing/monthly_purchase_events_by_brand_count/{data_period}"
    )
    processing_view_events_by_brand_count_path = (
        f"hdfs://namenode:9000/data/processing/monthly_view_events_by_brand_count/{data_period}"
    )
    processing_long_term_brand_popularity_index_path = (
        f"hdfs://namenode:9000/data/processing/monthly_long_term_brand_popularity_index/{data_period}"
    )

    spark = create_spark_session()

    cart_df = spark.read.option("header", "true").parquet(
        processing_cart_events_by_brand_count_path
    )
    purchase_df = spark.read.option("header", "true").parquet(
        processing_purchase_events_by_brand_count_path
    )
    view_df = spark.read.option("header", "true").parquet(
        processing_view_events_by_brand_count_path
    )

    scored_df = (
        view_df.alias("view")
        .join(cart_df.alias("cart"), on="brand", how="full")
        .join(purchase_df.alias("purchase"), on="brand", how="full")
        .select(
            coalesce(col("brand"), lit("unknown_brand")).alias("brand"),
            coalesce(col("view.view_events_count").cast("long"), lit(0)).alias(
                "view_events_count"
            ),
            coalesce(col("cart.cart_events_count").cast("long"), lit(0)).alias(
                "cart_events_count"
            ),
            coalesce(
                col("purchase.purchase_events_count").cast("long"), lit(0)
            ).alias("purchase_events_count"),
        )
        .withColumn(
            "long_term_brand_popularity_index",
            col("view_events_count") * lit(1)
            + col("cart_events_count") * lit(2)
            + col("purchase_events_count") * lit(10),
        )
        .withColumn("batch_processing", lit(data_period))
        .withColumn("data_period", lit(data_period))
        .orderBy(col("long_term_brand_popularity_index").desc(), col("brand").asc())
    )

    scored_df.write.mode("overwrite").option("header", "true").parquet(
        processing_long_term_brand_popularity_index_path
    )

    scored_df.write.format("jdbc").mode("overwrite").option(
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
    calculate_and_publish_monthly_long_term_brand_popularity_index(args.data_period)
