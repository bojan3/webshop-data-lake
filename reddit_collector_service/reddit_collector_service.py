import time
import requests
import json
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
from kafka import KafkaProducer

REDDIT_URL = "https://www.reddit.com/r/Smartphones/new.json?limit=5"
USER_AGENT = "reddit-collector-service/1.0"

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "reddit-comments"

LAST_RUN_FILE = "last_run.txt"
LOG_FILE = "logs/reddit_collector.log"
SLEEP_SECONDS = 300  # 5 minutes


def setup_logging():
    os.makedirs("logs", exist_ok=True)

    logger = logging.getLogger("reddit_collector")
    logger.setLevel(logging.INFO)

    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=10_000_000, backupCount=5
    )
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def load_last_run_timestamp() -> float:
    if not os.path.exists(LAST_RUN_FILE):
        return 0.0
    with open(LAST_RUN_FILE, "r") as f:
        return float(f.read().strip())


def save_last_run_timestamp(timestamp: float):
    with open(LAST_RUN_FILE, "w") as f:
        f.write(str(timestamp))


def fetch_new_posts():
    headers = {"User-Agent": USER_AGENT}
    response = requests.get(REDDIT_URL, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()["data"]["children"]


def fetch_comments(post_id: str):
    url = f"https://www.reddit.com/comments/{post_id}.json"
    headers = {"User-Agent": USER_AGENT}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    data = response.json()
    return [
        item["data"]
        for item in data[1]["data"]["children"]
        if item["kind"] == "t1"
    ]


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )


def main():
    logger = setup_logging()
    producer = create_kafka_producer()

    while True:
        logger.info("Starting Reddit collection cycle")
        last_run_ts = load_last_run_timestamp()
        current_run_ts = datetime.now(timezone.utc).timestamp()

        try:
            posts = fetch_new_posts()

            for post in posts:
                post_id = post["data"]["id"]
                comments = fetch_comments(post_id)

                for comment in comments:
                    if comment["created_utc"] > last_run_ts:
                        producer.send(
                            KAFKA_TOPIC,
                            {
                                "post_id": post_id,
                                "comment_id": comment["id"],
                                "author": comment.get("author"),
                                "text": comment.get("body"),
                                "created_utc": comment["created_utc"],
                            },
                        )
                        logger.info(
                            "Sent comment %s from post %s",
                            comment["id"],
                            post_id,
                        )

            producer.flush()
            save_last_run_timestamp(current_run_ts)

        except Exception:
            logger.exception("Error during Reddit collection cycle")

        logger.info("Cycle finished, sleeping for 5 minutes")
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()
