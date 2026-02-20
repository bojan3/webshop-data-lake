# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    split,
    sum as spark_sum,
    to_timestamp,
    trim,
    when,
)
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

DATA_PERIOD = "2020-Apr"
INPUT_FILE_NAME = f"{DATA_PERIOD}.csv"

INPUT_PATH = f"hdfs://namenode:9000/data/raw/{INPUT_FILE_NAME}"
PROCESSING_PURCHASES_PATH = f"hdfs://namenode:9000/data/processing/purchases/{DATA_PERIOD}"
PROCESSING_CLEAN_CATEGORIES_PATH = (
    f"hdfs://namenode:9000/data/processing/purchases_clean_categories/{DATA_PERIOD}"
)
PROCESSING_CATEGORY_REVENUE_SUM_PATH = (
    f"hdfs://namenode:9000/data/processing/category_revenue_sum/{DATA_PERIOD}"
)
CURATED_CATEGORY_REVENUE_RANK_PATH = (
    f"hdfs://namenode:9000/data/curated/category_revenue_rank/{DATA_PERIOD}"
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


def load_and_store_purchase_events() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").schema(get_schema()).csv(INPUT_PATH)
    df = df.withColumn(
        "event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss z")
    )
    purchase_df = df.filter(col("event_type") == "purchase")
    purchase_df.write.mode("overwrite").option("header", "true").csv(
        PROCESSING_PURCHASES_PATH
    )
    spark.stop()


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


def publish_category_revenue_rank() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(PROCESSING_CATEGORY_REVENUE_SUM_PATH)

    ranked_df = (
        df.select(
            "rank_category_name",
            col("total_revenue").cast("double").alias("total_revenue"),
        )
        .withColumn("batch_processing", lit(DATA_PERIOD))
        .orderBy(col("total_revenue").desc())
    )

    ranked_df.write.mode("overwrite").option("header", "true").csv(
        CURATED_CATEGORY_REVENUE_RANK_PATH
    )
    spark.stop()


def main() -> None:
    load_and_store_purchase_events()
    normalize_product_category_names()
    sum_prices_by_category()
    publish_category_revenue_rank()


if __name__ == "__main__":
    main()
