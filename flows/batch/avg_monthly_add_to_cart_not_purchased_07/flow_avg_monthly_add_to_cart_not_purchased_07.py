import requests
from prefect import flow, task


def trigger_spark_job(job_name: str):
    filename = f"batch/avg_monthly_add_to_cart_not_purchased_07/{job_name}.py"

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
def filter_and_store_processing_cart_events_task():
    return trigger_spark_job("filter_and_store_processing_cart_events")


@task
def filter_and_store_processing_purchase_events_task():
    return trigger_spark_job("filter_and_store_processing_purchase_events")


@task
def count_and_store_processing_cart_events_by_user_task():
    return trigger_spark_job("count_and_store_processing_cart_events_by_user")


@task
def count_and_store_processing_purchase_events_by_user_task():
    return trigger_spark_job("count_and_store_processing_purchase_events_by_user")


@task
def join_cart_and_purchase_counts_task():
    return trigger_spark_job("join_cart_and_purchase_counts")


@task
def publish_avg_cart_purchase_count_diff_task():
    return trigger_spark_job("publish_avg_cart_purchase_count_diff")


@flow(name="avg_monthly_add_to_cart_not_purchased_07")
def avg_monthly_add_to_cart_not_purchased_07():
    cart_filter_msg = filter_and_store_processing_cart_events_task.submit()
    purchase_filter_msg = filter_and_store_processing_purchase_events_task.submit()

    cart_count_msg = count_and_store_processing_cart_events_by_user_task.submit(
        wait_for=[cart_filter_msg]
    )
    purchase_count_msg = count_and_store_processing_purchase_events_by_user_task.submit(
        wait_for=[purchase_filter_msg]
    )

    join_msg = join_cart_and_purchase_counts_task.submit(
        wait_for=[cart_count_msg, purchase_count_msg]
    )
    publish_msg = publish_avg_cart_purchase_count_diff_task.submit(
        wait_for=[join_msg]
    )

    print(
        cart_filter_msg.result()
        + purchase_filter_msg.result()
        + cart_count_msg.result()
        + purchase_count_msg.result()
        + join_msg.result()
        + publish_msg.result()
    )


if __name__ == "__main__":
    avg_monthly_add_to_cart_not_purchased_07()
