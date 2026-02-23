import argparse

import requests
from prefect import flow, task


def trigger_spark_job(job_name: str, data_period: str):
    filename = f"batch/avg_hourly_product_views_08/{job_name}.py"

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
def load_and_store_processing_view_events_task(data_period: str):
    return trigger_spark_job("load_and_store_processing_view_events", data_period)


@task
def count_hourly_product_views_task(data_period: str):
    return trigger_spark_job("count_hourly_product_views", data_period)


@task
def calculate_and_publish_avg_hourly_product_views_task(data_period: str):
    return trigger_spark_job("calculate_and_publish_avg_hourly_product_views", data_period)


@flow(name="avg_hourly_product_views_08")
def avg_hourly_product_views_08(data_period: str):
    load_msg = load_and_store_processing_view_events_task.submit(data_period)
    count_msg = count_hourly_product_views_task.submit(data_period, wait_for=[load_msg])
    publish_msg = calculate_and_publish_avg_hourly_product_views_task.submit(
        data_period, wait_for=[count_msg]
    )

    print(load_msg.result() + count_msg.result() + publish_msg.result())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    avg_hourly_product_views_08(args.data_period)
