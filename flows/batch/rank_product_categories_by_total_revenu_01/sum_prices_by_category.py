# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "RankProductCategoriesByTotalRevenue"
    ).getOrCreate()


def sum_prices_by_category(data_period: str) -> None:
    processing_clean_categories_path = (
        f"hdfs://namenode:9000/data/processing/purchases_clean_categories/{data_period}"
    )
    processing_category_revenue_sum_path = (
        f"hdfs://namenode:9000/data/processing/category_revenue_sum/{data_period}"
    )

    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(processing_clean_categories_path)

    category_revenue_df = (
        df.groupBy("rank_category_name")
        .agg(spark_sum(col("price").cast("double")).alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
    )

    category_revenue_df.write.mode("overwrite").option("header", "true").csv(
        processing_category_revenue_sum_path
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    sum_prices_by_category(args.data_period)
