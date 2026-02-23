import argparse

import requests
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner


def trigger_spark_job(job_name: str, data_period: str):
    filename = f"batch/daily_revenue_and_units_sold_02/{job_name}.py"

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
def load_purchase_events_for_daily_metrics_task(data_period: str):
    return trigger_spark_job("load_purchase_events_for_daily_metrics", data_period)


@task
def calculate_daily_revenue_task(data_period: str):
    return trigger_spark_job("calculate_daily_revenue", data_period)


@task
def calculate_daily_units_sold_task(data_period: str):
    return trigger_spark_job("calculate_daily_units_sold", data_period)


@task
def publish_daily_sales_metrics_task(data_period: str):
    return trigger_spark_job("publish_daily_sales_metrics", data_period)


@flow(name="daily_revenue_and_units_sold_02", task_runner=ConcurrentTaskRunner())
def daily_revenue_and_units_sold_02(data_period: str):
    msg1 = load_purchase_events_for_daily_metrics_task.submit(data_period)
    msg2 = calculate_daily_revenue_task.submit(data_period, wait_for=[msg1])
    msg3 = calculate_daily_units_sold_task.submit(data_period, wait_for=[msg1])
    msg4 = publish_daily_sales_metrics_task.submit(data_period, wait_for=[msg2, msg3])
    print(msg1.result() + msg2.result() + msg3.result() + msg4.result())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    daily_revenue_and_units_sold_02(args.data_period)
