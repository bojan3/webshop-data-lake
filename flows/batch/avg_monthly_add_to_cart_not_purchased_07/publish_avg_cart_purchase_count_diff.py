# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, lit

DATA_PERIOD = "2020-Apr"

PROCESSING_CART_PURCHASE_COUNT_DIFF_PATH = (
    f"hdfs://namenode:9000/data/processing/monthly_cart_purchase_count_diff/{DATA_PERIOD}"
)
CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/webshop_data_lake"
CLICKHOUSE_TABLE = "avg_cart_purchase_count_diff"
CLICKHOUSE_USER = "analytics"
CLICKHOUSE_PASSWORD = "analytics123"


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(
        "AvgMonthlyAddToCartNotPurchased-Apr2020"
    ).getOrCreate()


def publish_avg_cart_purchase_count_diff() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(PROCESSING_CART_PURCHASE_COUNT_DIFF_PATH)

    avg_diff_df = (
        df.agg(
            avg(col("cart_purchase_count_diff").cast("double")).alias(
                "avg_cart_purchase_count_diff"
            )
        )
        .withColumn("batch_processing", lit(DATA_PERIOD))
        .select("avg_cart_purchase_count_diff", "batch_processing")
    )

    avg_diff_df.write.format("jdbc").mode("overwrite").option(
        "url", CLICKHOUSE_URL
    ).option("dbtable", CLICKHOUSE_TABLE).option(
        "driver", "com.clickhouse.jdbc.ClickHouseDriver"
    ).option("user", CLICKHOUSE_USER).option("password", CLICKHOUSE_PASSWORD).option(
        "createTableOptions", "ENGINE = MergeTree() ORDER BY batch_processing"
    ).save()
    spark.stop()


if __name__ == "__main__":
    publish_avg_cart_purchase_count_diff()
