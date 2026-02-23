import argparse

import requests
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner


def trigger_spark_job(job_name: str, data_period: str):
    filename = f"batch/long_term_brand_popularity_index_10/{job_name}.py"

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
def load_processing_cart_events_task(data_period: str):
    return trigger_spark_job("load_processing_cart_events", data_period)


@task
def load_processing_purchase_events_task(data_period: str):
    return trigger_spark_job("load_processing_purchase_events", data_period)


@task
def load_processing_view_events_task(data_period: str):
    return trigger_spark_job("load_processing_view_events", data_period)


@task
def count_processing_cart_events_by_brand_task(data_period: str):
    return trigger_spark_job("count_processing_cart_events_by_brand", data_period)


@task
def count_processing_purchase_events_by_brand_task(data_period: str):
    return trigger_spark_job("count_processing_purchase_events_by_brand", data_period)


@task
def count_processing_view_events_by_brand_task(data_period: str):
    return trigger_spark_job("count_processing_view_events_by_brand", data_period)


@task
def calculate_and_publish_monthly_long_term_brand_popularity_index_task(data_period: str):
    return trigger_spark_job(
        "calculate_and_publish_monthly_long_term_brand_popularity_index", data_period
    )


@task
def sum_all_previous_months_and_publish_long_term_brand_popularity_index_task(data_period: str):
    return trigger_spark_job(
        "sum_all_previous_months_and_publish_long_term_brand_popularity_index",
        data_period,
    )


@flow(name="long_term_brand_popularity_index_10", task_runner=ConcurrentTaskRunner())
def long_term_brand_popularity_index_10(data_period: str):
    load_cart_msg = load_processing_cart_events_task.submit(data_period)
    load_purchase_msg = load_processing_purchase_events_task.submit(data_period)
    load_view_msg = load_processing_view_events_task.submit(data_period)

    count_cart_msg = count_processing_cart_events_by_brand_task.submit(
        data_period, wait_for=[load_cart_msg]
    )
    count_purchase_msg = count_processing_purchase_events_by_brand_task.submit(
        data_period, wait_for=[load_purchase_msg]
    )
    count_view_msg = count_processing_view_events_by_brand_task.submit(
        data_period, wait_for=[load_view_msg]
    )

    publish_monthly_msg = (
        calculate_and_publish_monthly_long_term_brand_popularity_index_task.submit(
            data_period, wait_for=[count_cart_msg, count_purchase_msg, count_view_msg]
        )
    )

    publish_all_months_msg = (
        sum_all_previous_months_and_publish_long_term_brand_popularity_index_task.submit(
            data_period, wait_for=[publish_monthly_msg]
        )
    )

    print(
        load_cart_msg.result()
        + load_purchase_msg.result()
        + load_view_msg.result()
        + count_cart_msg.result()
        + count_purchase_msg.result()
        + count_view_msg.result()
        + publish_monthly_msg.result()
        + publish_all_months_msg.result()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    long_term_brand_popularity_index_10(args.data_period)
