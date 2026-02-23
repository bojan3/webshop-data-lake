# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, when


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("RankBrandsByTotalRevenue").getOrCreate()


def normalize_brand_names(data_period: str) -> None:
    processing_purchases_path = (
        f"hdfs://namenode:9000/data/processing/brand_purchases/{data_period}"
    )
    processing_clean_brands_path = (
        f"hdfs://namenode:9000/data/processing/purchases_clean_brands/{data_period}"
    )

    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(processing_purchases_path)

    cleaned_df = df.withColumn(
        "rank_brand_name",
        when(col("brand").isNull() | (trim(col("brand")) == ""), "WITHOUT BRAND").otherwise(
            upper(trim(col("brand")))
        ),
    )

    cleaned_df.write.mode("overwrite").option("header", "true").csv(
        processing_clean_brands_path
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    normalize_brand_names(args.data_period)
