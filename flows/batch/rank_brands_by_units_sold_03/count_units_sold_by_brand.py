# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

DATA_PERIOD = "2020-Apr"

PROCESSING_CLEAN_BRANDS_PATH = (
    f"hdfs://namenode:9000/data/processing/purchases_clean_brands/{DATA_PERIOD}"
)
PROCESSING_BRAND_UNITS_SOLD_PATH = (
    f"hdfs://namenode:9000/data/processing/brand_units_sold/{DATA_PERIOD}"
)

def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("RankBrandsByUnitsSold-Apr2020").getOrCreate()


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


if __name__ == "__main__":
    count_units_sold_by_brand()
