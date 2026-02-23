# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, trim, when
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "RankProductCategoriesByTotalRevenue"
    ).getOrCreate()


def get_schema() -> StructType:
    return StructType(
        [
            StructField("event_time", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", LongType(), True),
            StructField("category_id", LongType(), True),
            StructField("category_code", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("user_id", LongType(), True),
            StructField("user_session", StringType(), True),
        ]
    )


def normalize_product_category_names(data_period: str) -> None:
    processing_purchases_path = (
        f"hdfs://namenode:9000/data/processing/purchases/{data_period}"
    )
    processing_clean_categories_path = (
        f"hdfs://namenode:9000/data/processing/purchases_clean_categories/{data_period}"
    )

    spark = create_spark_session()
    df = spark.read.option("header", "true").schema(get_schema()).csv(
        processing_purchases_path
    )
    cleaned_df = df.withColumn(
        "rank_category_name",
        when(
            col("category_code").isNull() | (trim(col("category_code")) == ""),
            "without category",
        ).otherwise(split(col("category_code"), r"\\.").getItem(0)),
    )
    cleaned_df.write.mode("overwrite").option("header", "true").csv(
        processing_clean_categories_path
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    normalize_product_category_names(args.data_period)
