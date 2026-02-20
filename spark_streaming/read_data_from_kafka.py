from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    current_timestamp,
    date_format,
)
from pyspark.sql.types import StructType, StringType, LongType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "reddit-comments"

BASE_RAW_PATH = "hdfs://namenode:9000/data/raw/reddit_comments"
CHECKPOINT_PATH = "hdfs://namenode:9000/checkpoints/reddit_comments"


def main():
    spark = (
        SparkSession.builder
        .appName("KafkaToRawStreaming")
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
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), schema).alias("data"))
        .select("data.*")
        .withColumn("ingest_ts", current_timestamp())
        .withColumn("ingest_date", date_format(col("ingest_ts"), "yyyy-MM-dd"))
        .withColumn("ingest_time", date_format(col("ingest_ts"), "HH-mm-ss"))
    )

    query = (
        parsed_df
        .writeStream
        .format("csv")
        .outputMode("append")
        .option("path", BASE_RAW_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("header", "true")
        .partitionBy("ingest_date", "ingest_time")
        .trigger(processingTime="1 minute")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()