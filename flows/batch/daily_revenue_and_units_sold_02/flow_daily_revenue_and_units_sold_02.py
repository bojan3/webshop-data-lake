import requests
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner


def trigger_spark_job(job_name: str):
    filename = f"batch/daily_revenue_and_units_sold_02/{job_name}.py"

    response = requests.post(
        "http://spark-master:8000/run",
        params={"filename": filename}
    )

    response.raise_for_status()

    data = response.json()

    if not data.get("success"):
        raise RuntimeError(data.get("message"))

    return data["message"]


@task
def load_purchase_events_for_daily_metrics_task():
    return trigger_spark_job("load_purchase_events_for_daily_metrics")


@task
def calculate_daily_revenue_task():
    return trigger_spark_job("calculate_daily_revenue")


@task
def calculate_daily_units_sold_task():
    return trigger_spark_job("calculate_daily_units_sold")


@task
def publish_daily_sales_metrics_task():
    return trigger_spark_job("publish_daily_sales_metrics")


@flow(name="daily_revenue_and_units_sold_02", task_runner=ConcurrentTaskRunner())
def daily_revenue_and_units_sold_02():
    msg1 = load_purchase_events_for_daily_metrics_task.submit()
    msg2 = calculate_daily_revenue_task.submit(wait_for=[msg1])
    msg3 = calculate_daily_units_sold_task.submit(wait_for=[msg1])
    msg4 = publish_daily_sales_metrics_task.submit(wait_for=[msg2, msg3])
    print(msg1.result() + msg2.result() + msg3.result() + msg4.result())


if __name__ == "__main__":
    daily_revenue_and_units_sold_02()
