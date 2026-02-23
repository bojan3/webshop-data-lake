import argparse

import requests
from prefect import flow, task


def trigger_spark_job(job_name: str, data_period: str):
    filename = f"batch/avg_monthly_add_to_cart_not_purchased_07/{job_name}.py"

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
def filter_and_store_processing_cart_events_task(data_period: str):
    return trigger_spark_job("filter_and_store_processing_cart_events", data_period)


@task
def filter_and_store_processing_purchase_events_task(data_period: str):
    return trigger_spark_job("filter_and_store_processing_purchase_events", data_period)


@task
def count_and_store_processing_cart_events_by_user_task(data_period: str):
    return trigger_spark_job("count_and_store_processing_cart_events_by_user", data_period)


@task
def count_and_store_processing_purchase_events_by_user_task(data_period: str):
    return trigger_spark_job("count_and_store_processing_purchase_events_by_user", data_period)


@task
def join_cart_and_purchase_counts_task(data_period: str):
    return trigger_spark_job("join_cart_and_purchase_counts", data_period)


@task
def publish_avg_cart_purchase_count_diff_task(data_period: str):
    return trigger_spark_job("publish_avg_cart_purchase_count_diff", data_period)


@flow(name="avg_monthly_add_to_cart_not_purchased_07")
def avg_monthly_add_to_cart_not_purchased_07(data_period: str):
    cart_filter_msg = filter_and_store_processing_cart_events_task.submit(data_period)
    purchase_filter_msg = filter_and_store_processing_purchase_events_task.submit(
        data_period
    )

    cart_count_msg = count_and_store_processing_cart_events_by_user_task.submit(
        data_period, wait_for=[cart_filter_msg]
    )
    purchase_count_msg = count_and_store_processing_purchase_events_by_user_task.submit(
        data_period, wait_for=[purchase_filter_msg]
    )

    join_msg = join_cart_and_purchase_counts_task.submit(
        data_period, wait_for=[cart_count_msg, purchase_count_msg]
    )
    publish_msg = publish_avg_cart_purchase_count_diff_task.submit(
        data_period, wait_for=[join_msg]
    )

    print(
        cart_filter_msg.result()
        + purchase_filter_msg.result()
        + cart_count_msg.result()
        + purchase_count_msg.result()
        + join_msg.result()
        + publish_msg.result()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    avg_monthly_add_to_cart_not_purchased_07(args.data_period)
