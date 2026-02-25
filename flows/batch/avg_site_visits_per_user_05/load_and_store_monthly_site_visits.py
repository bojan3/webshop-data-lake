# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("AvgSiteVisitsPerUser").getOrCreate()


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


def load_and_store_monthly_site_visits(data_period: str) -> None:
    input_file_name = f"{data_period}.csv"
    input_path = f"hdfs://namenode:9000/data/raw/{input_file_name}"
    processing_monthly_site_visits_path = (
        f"hdfs://namenode:9000/data/processing/monthly_site_visits/{data_period}"
    )

    spark = create_spark_session()
    df = spark.read.option("header", "true").schema(get_schema()).csv(input_path)
    monthly_site_visits_df = (
        df.filter(col("user_id").isNotNull() & col("user_session").isNotNull())
        .select("user_id", "user_session")
        .distinct()
    )
    monthly_site_visits_df.write.mode("overwrite").option("header", "true").parquet(
        processing_monthly_site_visits_path
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    load_and_store_monthly_site_visits(args.data_period)
