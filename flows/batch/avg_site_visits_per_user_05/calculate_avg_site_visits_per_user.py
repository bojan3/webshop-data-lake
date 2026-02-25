# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, lit


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("AvgSiteVisitsPerUser").getOrCreate()


def calculate_avg_site_visits_per_user(data_period: str) -> None:
    processing_monthly_site_visits_by_user_count_path = (
        f"hdfs://namenode:9000/data/processing/monthly_site_visits_by_user_count/{data_period}"
    )
    processing_monthly_avg_site_visits_per_user_path = (
        f"hdfs://namenode:9000/data/processing/monthly_avg_site_visits_per_user/{data_period}"
    )

    spark = create_spark_session()
    df = spark.read.option("header", "true").parquet(
        processing_monthly_site_visits_by_user_count_path
    )

    avg_site_visits_df = (
        df.agg(avg(col("site_visit_count").cast("double")).alias("avg_site_visits_per_user"))
        .withColumn("batch_processing", lit(data_period))
        .select("avg_site_visits_per_user", "batch_processing")
    )

    avg_site_visits_df.write.mode("overwrite").option("header", "true").parquet(
        processing_monthly_avg_site_visits_per_user_path
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    calculate_avg_site_visits_per_user(args.data_period)
