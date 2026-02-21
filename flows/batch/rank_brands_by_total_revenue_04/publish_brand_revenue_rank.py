# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

DATA_PERIOD = "2020-Apr"

PROCESSING_BRAND_REVENUE_SUM_PATH = (
    f"hdfs://namenode:9000/data/processing/brand_revenue_sum/{DATA_PERIOD}"
)
CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/webshop_data_lake"
CLICKHOUSE_TABLE = "brand_revenue_rank"
CLICKHOUSE_USER = "analytics"
CLICKHOUSE_PASSWORD = "analytics123"


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("RankBrandsByTotalRevenue-Apr2020").getOrCreate()


def publish_brand_revenue_rank() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(PROCESSING_BRAND_REVENUE_SUM_PATH)

    ranked_df = (
        df.select(
            "rank_brand_name",
            col("total_revenue").cast("double").alias("total_revenue"),
        )
        .withColumn("batch_processing", lit(DATA_PERIOD))
        .orderBy(col("total_revenue").desc())
    )

    ranked_df.write.format("jdbc").mode("overwrite").option(
        "url", CLICKHOUSE_URL
    ).option("dbtable", CLICKHOUSE_TABLE).option(
        "driver", "com.clickhouse.jdbc.ClickHouseDriver"
    ).option("user", CLICKHOUSE_USER).option("password", CLICKHOUSE_PASSWORD).option(
        "createTableOptions", "ENGINE = MergeTree() ORDER BY rank_brand_name"
    ).save()
    spark.stop()


if __name__ == "__main__":
    publish_brand_revenue_rank()
