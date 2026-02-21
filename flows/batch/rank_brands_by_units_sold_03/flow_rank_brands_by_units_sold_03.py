import requests
from prefect import flow, task


def trigger_spark_job(job_name: str):
    filename = f"batch/rank_brands_by_units_sold_03/{job_name}.py"

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
def normalize_brand_names_task():
    return trigger_spark_job("normalize_brand_names")


@task
def count_units_sold_by_brand_task():
    return trigger_spark_job("count_units_sold_by_brand")


@task
def publish_brand_units_sold_rank_task():
    return trigger_spark_job("publish_brand_units_sold_rank")


@flow(name="rank_brands_by_units_sold_03")
def rank_brands_by_units_sold_03():
    msg1 = load_and_store_purchase_events_task()
    msg2 = normalize_brand_names_task()
    msg3 = count_units_sold_by_brand_task()
    msg4 = publish_brand_units_sold_rank_task()
    print(msg1 + msg2 + msg3 + msg4)


if __name__ == "__main__":
    rank_brands_by_units_sold_03()
