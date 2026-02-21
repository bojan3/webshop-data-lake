# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, when

DATA_PERIOD = "2020-Apr"

PROCESSING_PURCHASES_PATH = (
    f"hdfs://namenode:9000/data/processing/brand_purchases/{DATA_PERIOD}"
)
PROCESSING_CLEAN_BRANDS_PATH = (
    f"hdfs://namenode:9000/data/processing/purchases_clean_brands/{DATA_PERIOD}"
)


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("RankBrandsByTotalRevenue-Apr2020").getOrCreate()


def normalize_brand_names() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(PROCESSING_PURCHASES_PATH)

    cleaned_df = df.withColumn(
        "rank_brand_name",
        when(col("brand").isNull() | (trim(col("brand")) == ""), "WITHOUT BRAND").otherwise(
            upper(trim(col("brand")))
        ),
    )

    cleaned_df.write.mode("overwrite").option("header", "true").csv(
        PROCESSING_CLEAN_BRANDS_PATH
    )
    spark.stop()


if __name__ == "__main__":
    normalize_brand_names()
