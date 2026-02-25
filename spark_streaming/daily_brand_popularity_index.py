import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    lit,
    regexp_replace,
    trim,
    udf,
    upper,
)
from pyspark.sql.types import LongType, StringType, StructType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "reddit-comments"
MODEL_CATALOG_PATH = "hdfs://namenode:9000/data/raw/products_merged_all_brands.csv"
CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/webshop_data_lake"
CLICKHOUSE_TABLE = "live_long_term_brand_popularity_index_total"
CLICKHOUSE_USER = "analytics"
CLICKHOUSE_PASSWORD = "analytics123"


def main():
    spark = (
        SparkSession.builder
        .appName("daily_brand_popularity_index")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    schema = (
        StructType()
        .add("post_id", StringType())
        .add("comment_id", StringType())
        .add("author", StringType())
        .add("text", StringType())
        .add("created_utc", LongType())
    )

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), schema).alias("data"))
        .select("data.*")
    )

    model_catalog_df = (
        spark.read.option("header", "true")
        .csv(MODEL_CATALOG_PATH)
        .select("item_label", "brand")
        .where(col("item_label").isNotNull() & col("brand").isNotNull())
        .withColumn(
            "item_label_upper",
            trim(
                regexp_replace(
                    upper(col("item_label")),
                    r"[^A-Z0-9]+",
                    " ",
                )
            ),
        )
        .withColumn(
            "brand_upper",
            upper(col("brand")),
        )
        .where(col("item_label_upper") != "")
        .where(col("brand_upper") != "")
        .distinct()
    )

    label_to_brand_dictionary = {
        row.item_label_upper: row.brand_upper
        for row in model_catalog_df.select("item_label_upper", "brand_upper").collect()
    }
    item_labels = sorted(label_to_brand_dictionary.keys(), key=len, reverse=True)

    def find_matched_item_label(text_upper: str) -> str:
        if not text_upper:
            return ""
        for item_label in item_labels:
            if item_label in text_upper:
                return item_label
        return ""

    def find_matched_brand(item_label: str) -> str:
        if not item_label:
            return None
        return label_to_brand_dictionary.get(item_label)

    find_matched_item_label_udf = udf(find_matched_item_label, StringType())
    find_matched_brand_udf = udf(find_matched_brand, StringType())

    parsed_df = (
        parsed_df.withColumn(
            "text_upper",
            upper(trim(col("text"))),
        )
        .withColumn(
            "matched_item_label",
            find_matched_item_label_udf(col("text_upper")),
        )
        .withColumn(
            "matched_brand",
            find_matched_brand_udf(col("matched_item_label")),
        )
    )

    def write_matched_brand_points(batch_df, _batch_id: int) -> None:
        brand_points_df = (
            batch_df
            .where(col("matched_brand").isNotNull() & (col("matched_brand") != ""))
            .select(
                col("matched_brand").alias("brand"),
                lit(10).cast("long").alias("total_long_term_brand_popularity_index"),
            )
        )

        if not brand_points_df.head(1):
            return

        brand_points_df.write.format("jdbc").mode("append").option(
            "url", CLICKHOUSE_URL
        ).option("dbtable", CLICKHOUSE_TABLE).option(
            "driver", "com.clickhouse.jdbc.ClickHouseDriver"
        ).option("user", CLICKHOUSE_USER).option(
            "password", CLICKHOUSE_PASSWORD
        ).option(
            "createTableOptions", "ENGINE = MergeTree() ORDER BY brand"
        ).save()

    query = (
        parsed_df.writeStream
        .outputMode("append")
        .foreachBatch(write_matched_brand_points)
        .trigger(processingTime="1 minute")
        .start()
    )

    query.awaitTermination()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=False)
    return parser.parse_args()


if __name__ == "__main__":
    parse_args()
    main()
