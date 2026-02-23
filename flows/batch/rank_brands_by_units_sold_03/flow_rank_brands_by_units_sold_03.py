import argparse

import requests
from prefect import flow, task


def trigger_spark_job(job_name: str, data_period: str):
    filename = f"batch/rank_brands_by_units_sold_03/{job_name}.py"

    response = requests.post(
        "http://spark-master:8000/run",
        params={"filename": filename, "data_period": data_period}
    )

    response.raise_for_status()

    data = response.json()

    if not data.get("success"):
        raise RuntimeError(data.get("message"))

    return data["message"]


@task
def load_and_store_purchase_events_task(data_period: str):
    return trigger_spark_job("load_and_store_purchase_events", data_period)


@task
def normalize_brand_names_task(data_period: str):
    return trigger_spark_job("normalize_brand_names", data_period)


@task
def count_units_sold_by_brand_task(data_period: str):
    return trigger_spark_job("count_units_sold_by_brand", data_period)


@task
def publish_brand_units_sold_rank_task(data_period: str):
    return trigger_spark_job("publish_brand_units_sold_rank", data_period)


@flow(name="rank_brands_by_units_sold_03")
def rank_brands_by_units_sold_03(data_period: str):
    msg1 = load_and_store_purchase_events_task.submit(data_period)
    msg2 = normalize_brand_names_task.submit(data_period, wait_for=[msg1])
    msg3 = count_units_sold_by_brand_task.submit(data_period, wait_for=[msg2])
    msg4 = publish_brand_units_sold_rank_task.submit(data_period, wait_for=[msg3])
    print(msg1.result() + msg2.result() + msg3.result() + msg4.result())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    rank_brands_by_units_sold_03(args.data_period)
