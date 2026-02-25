# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("AvgSiteVisitsPerUser").getOrCreate()


def count_site_visits_by_user(data_period: str) -> None:
    processing_monthly_site_visits_path = (
        f"hdfs://namenode:9000/data/processing/monthly_site_visits/{data_period}"
    )
    processing_monthly_site_visits_by_user_count_path = (
        f"hdfs://namenode:9000/data/processing/monthly_site_visits_by_user_count/{data_period}"
    )

    spark = create_spark_session()
    df = spark.read.option("header", "true").parquet(processing_monthly_site_visits_path)

    site_visits_by_user_df = (
        df.groupBy("user_id")
        .agg(count(col("user_session")).alias("site_visit_count"))
        .select(col("user_id").cast("long"), "site_visit_count")
    )

    site_visits_by_user_df.write.mode("overwrite").option("header", "true").parquet(
        processing_monthly_site_visits_by_user_count_path
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    count_site_visits_by_user(args.data_period)
