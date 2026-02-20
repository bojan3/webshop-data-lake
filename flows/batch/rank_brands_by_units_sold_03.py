# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, to_timestamp, trim, upper, when
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

DATA_PERIOD = "2020-Apr"
INPUT_FILE_NAME = f"{DATA_PERIOD}.csv"

INPUT_PATH = f"hdfs://namenode:9000/data/raw/{INPUT_FILE_NAME}"
PROCESSING_PURCHASES_PATH = (
    f"hdfs://namenode:9000/data/processing/brand_purchases/{DATA_PERIOD}"
)
PROCESSING_CLEAN_BRANDS_PATH = (
    f"hdfs://namenode:9000/data/processing/purchases_clean_brands/{DATA_PERIOD}"
)
PROCESSING_BRAND_UNITS_SOLD_PATH = (
    f"hdfs://namenode:9000/data/processing/brand_units_sold/{DATA_PERIOD}"
)
CURATED_BRAND_UNITS_SOLD_RANK_PATH = (
    f"hdfs://namenode:9000/data/curated/brand_units_sold_rank/{DATA_PERIOD}"
)


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("RankBrandsByUnitsSold-Apr2020").getOrCreate()


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


def count_units_sold_by_brand() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(PROCESSING_CLEAN_BRANDS_PATH)

    brand_units_sold_df = (
        df.groupBy("rank_brand_name")
        .agg(count(col("product_id")).alias("units_sold"))
        .orderBy(col("units_sold").desc())
    )

    brand_units_sold_df.write.mode("overwrite").option("header", "true").csv(
        PROCESSING_BRAND_UNITS_SOLD_PATH
    )
    spark.stop()


def publish_brand_units_sold_rank() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(PROCESSING_BRAND_UNITS_SOLD_PATH)

    ranked_df = (
        df.select(
            "rank_brand_name",
            col("units_sold").cast("long").alias("units_sold"),
        )
        .withColumn("batch_processing", lit(DATA_PERIOD))
        .orderBy(col("units_sold").desc())
    )

    ranked_df.write.mode("overwrite").option("header", "true").csv(
        CURATED_BRAND_UNITS_SOLD_RANK_PATH
    )
    spark.stop()


def main() -> None:
    load_and_store_purchase_events()
    normalize_brand_names()
    count_units_sold_by_brand()
    publish_brand_units_sold_rank()


if __name__ == "__main__":
    main()
