# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("RankElectronicsBrandsByUnitsSold").getOrCreate()


def count_electronics_units_sold_by_brand(data_period: str) -> None:
    processing_clean_brands_path = (
        f"hdfs://namenode:9000/data/processing/electronics_purchases_clean_brands/{data_period}"
    )
    processing_brand_units_sold_path = (
        f"hdfs://namenode:9000/data/processing/electronics_brand_units_sold/{data_period}"
    )

    spark = create_spark_session()
    df = spark.read.option("header", "true").parquet(processing_clean_brands_path)

    brand_units_sold_df = (
        df.groupBy("rank_brand_name")
        .agg(count(col("product_id")).alias("units_sold"))
        .orderBy(col("units_sold").desc())
    )

    brand_units_sold_df.write.mode("overwrite").option("header", "true").parquet(
        processing_brand_units_sold_path
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    count_electronics_units_sold_by_brand(args.data_period)
