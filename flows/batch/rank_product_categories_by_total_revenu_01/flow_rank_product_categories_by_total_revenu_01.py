import requests
from prefect import flow, task


def trigger_spark_job(job_name: str):
    filename = f"batch/rank_product_categories_by_total_revenu_01/{job_name}.py"

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
def load_and_store_purchase_events_task():
    return trigger_spark_job("load_and_store_purchase_events")


@task
def normalize_product_category_names_task():
    return trigger_spark_job("normalize_product_category_names")


@task
def sum_prices_by_category_task():
    return trigger_spark_job("sum_prices_by_category")


@task
def publish_category_revenue_rank_task():
    return trigger_spark_job("publish_category_revenue_rank")


@flow(name="rank_product_categories_by_total_revenue_01")
def rank_product_categories_by_total_revenue_01():
    msg1 = load_and_store_purchase_events_task()
    msg2 = normalize_product_category_names_task()
    msg3 = sum_prices_by_category_task()
    msg4 = publish_category_revenue_rank_task()
    print(msg1 + msg2 + msg3 + msg4)


if __name__ == "__main__":
    rank_product_categories_by_total_revenue_01()
