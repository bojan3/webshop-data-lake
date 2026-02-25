import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import LongType, StringType, StructType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "reddit-comments"


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

    query = (
        parsed_df
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
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
