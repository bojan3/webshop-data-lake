# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, lit, to_date, to_timestamp
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

DATA_PERIOD = "2020-Apr"
INPUT_FILE_NAME = f"{DATA_PERIOD}.csv"

INPUT_PATH = f"hdfs://namenode:9000/data/raw/{INPUT_FILE_NAME}"
PROCESSING_DAILY_PURCHASE_EVENTS_PATH = (
    f"hdfs://namenode:9000/data/processing/daily_purchase_events/{DATA_PERIOD}"
)
PROCESSING_DAILY_SALES_SUMMARY_PATH = (
    f"hdfs://namenode:9000/data/processing/daily_sales_summary/{DATA_PERIOD}"
)
PROCESSING_DAILY_REVENUE_PATH = (
    f"hdfs://namenode:9000/data/processing/daily_revenue/{DATA_PERIOD}"
)
PROCESSING_DAILY_UNITS_SOLD_PATH = (
    f"hdfs://namenode:9000/data/processing/daily_units_sold/{DATA_PERIOD}"
)
CURATED_DAILY_REVENUE_AND_UNITS_SOLD_PATH = (
    f"hdfs://namenode:9000/data/curated/daily_revenue_and_units_sold/{DATA_PERIOD}"
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



def load_purchase_events_for_daily_metrics() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").schema(get_schema()).csv(INPUT_PATH)
    df = df.withColumn(
        "event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss z")
    )
    purchase_df = df.filter(col("event_type") == "purchase")
    purchase_df.write.mode("overwrite").option("header", "true").csv(
        PROCESSING_DAILY_PURCHASE_EVENTS_PATH
    )
    spark.stop()


def calculate_daily_revenue() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(PROCESSING_DAILY_PURCHASE_EVENTS_PATH)

    daily_revenue_df = (
        df.withColumn("event_date", to_date(col("event_time")))
        .groupBy("event_date")
        .agg(avg(col("price").cast("double")).alias("average_daily_revenue"))
        .orderBy(col("event_date").asc())
    )

    daily_revenue_df.write.mode("overwrite").option("header", "true").csv(
        PROCESSING_DAILY_REVENUE_PATH
    )
    spark.stop()


def calculate_daily_units_sold() -> None:
    spark = create_spark_session()
    df = spark.read.option("header", "true").csv(PROCESSING_DAILY_PURCHASE_EVENTS_PATH)

    daily_units_sold_df = (
        df.withColumn("event_date", to_date(col("event_time")))
        .groupBy("event_date")
        .agg(count(col("product_id")).alias("units_sold"))
        .orderBy(col("event_date").asc())
    )

    daily_units_sold_df.write.mode("overwrite").option("header", "true").csv(
        PROCESSING_DAILY_UNITS_SOLD_PATH
    )
    spark.stop()


def publish_daily_sales_metrics() -> None:
    spark = create_spark_session()
    revenue_df = spark.read.option("header", "true").csv(PROCESSING_DAILY_REVENUE_PATH)
    units_sold_df = spark.read.option("header", "true").csv(
        PROCESSING_DAILY_UNITS_SOLD_PATH
    )

    published_df = (
        revenue_df.join(units_sold_df, on="event_date", how="inner")
        .select(
            to_date(col("event_date")).alias("event_date"),
            col("average_daily_revenue").cast("double").alias("average_daily_revenue"),
            col("units_sold").cast("long").alias("units_sold"),
        )
        .withColumn("batch_processing", lit(DATA_PERIOD))
        .orderBy(col("event_date").asc())
    )

    published_df.write.mode("overwrite").option("header", "true").csv(
        CURATED_DAILY_REVENUE_AND_UNITS_SOLD_PATH
    )
    spark.stop()


def main() -> None:
    load_purchase_events_for_daily_metrics()
    calculate_daily_revenue()
    calculate_daily_units_sold()
    publish_daily_sales_metrics()


if __name__ == "__main__":
    main()
