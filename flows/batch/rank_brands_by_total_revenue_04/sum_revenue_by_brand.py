# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

DATA_PERIOD = "2020-Apr"

PROCESSING_CLEAN_BRANDS_PATH = (
    f"hdfs://namenode:9000/data/processing/purchases_clean_brands/{DATA_PERIOD}"
)
PROCESSING_BRAND_REVENUE_SUM_PATH = (
    f"hdfs://namenode:9000/data/processing/brand_revenue_sum/{DATA_PERIOD}"
)


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("RankBrandsByTotalRevenue-Apr2020").getOrCreate()


def sum_revenue_by_brand() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(PROCESSING_CLEAN_BRANDS_PATH)

    brand_revenue_df = (
        df.groupBy("rank_brand_name")
        .agg(spark_sum(col("price").cast("double")).alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
    )

    brand_revenue_df.write.mode("overwrite").option("header", "true").csv(
        PROCESSING_BRAND_REVENUE_SUM_PATH
    )
    spark.stop()


if __name__ == "__main__":
    sum_revenue_by_brand()

