# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
)

DATA_PERIOD = "2020-Apr"
PROCESSING_CLEAN_CATEGORIES_PATH = (
    f"hdfs://namenode:9000/data/processing/purchases_clean_categories/{DATA_PERIOD}"
)
PROCESSING_CATEGORY_REVENUE_SUM_PATH = (
    f"hdfs://namenode:9000/data/processing/category_revenue_sum/{DATA_PERIOD}"
)

def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "RankProductCategoriesByTotalRevenue-Apr2020"
    ).getOrCreate()

def sum_prices_by_category() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(PROCESSING_CLEAN_CATEGORIES_PATH)

    category_revenue_df = (
        df.groupBy("rank_category_name")
        .agg(spark_sum(col("price").cast("double")).alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
    )

    category_revenue_df.write.mode("overwrite").option("header", "true").csv(
        PROCESSING_CATEGORY_REVENUE_SUM_PATH
    )
    spark.stop()

if __name__ == "__main__":
    sum_prices_by_category()