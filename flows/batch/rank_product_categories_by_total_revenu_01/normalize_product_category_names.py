# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    split,
    trim,
    when,
)
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


DATA_PERIOD = "2020-Apr"

PROCESSING_PURCHASES_PATH = f"hdfs://namenode:9000/data/processing/purchases/{DATA_PERIOD}"
PROCESSING_CLEAN_CATEGORIES_PATH = (
    f"hdfs://namenode:9000/data/processing/purchases_clean_categories/{DATA_PERIOD}"
)

def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "RankProductCategoriesByTotalRevenue-Apr2020"
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


def normalize_product_category_names() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").schema(get_schema()).csv(
        PROCESSING_PURCHASES_PATH
    )
    cleaned_df = df.withColumn(
        "rank_category_name",
        when(
            col("category_code").isNull() | (trim(col("category_code")) == ""),
            "without category",
        ).otherwise(split(col("category_code"), r"\.").getItem(0)),
    )
    cleaned_df.write.mode("overwrite").option("header", "true").csv(
        PROCESSING_CLEAN_CATEGORIES_PATH
    )
    spark.stop()

    if __name__ == "__main__":
        normalize_product_category_names()
