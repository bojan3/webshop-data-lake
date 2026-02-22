import requests
from prefect import flow, task


def trigger_spark_job(job_name: str):
    filename = f"batch/avg_hourly_product_views_08/{job_name}.py"

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
def load_and_store_processing_view_events_task():
    return trigger_spark_job("load_and_store_processing_view_events")


@task
def count_hourly_product_views_task():
    return trigger_spark_job("count_hourly_product_views")


@task
def calculate_and_publish_avg_hourly_product_views_task():
    return trigger_spark_job("calculate_and_publish_avg_hourly_product_views")


@flow(name="avg_hourly_product_views_08")
def avg_hourly_product_views_08():
    load_msg = load_and_store_processing_view_events_task.submit()
    count_msg = count_hourly_product_views_task.submit(wait_for=[load_msg])
    publish_msg = calculate_and_publish_avg_hourly_product_views_task.submit(
        wait_for=[count_msg]
    )

    print(load_msg.result() + count_msg.result() + publish_msg.result())


if __name__ == "__main__":
    avg_hourly_product_views_08()
