import requests
from prefect import flow, task

DUMMY_VALUE = "dummy_value"

def trigger_spark_job(job_name: str):
    filename = f"realtime/{job_name}.py"

    response = requests.post(
        "http://spark-master:8000/run",
        params={"filename": filename, "data_period": f"DUMMY_VALUE={DUMMY_VALUE}"},
    )
    response.raise_for_status()

    data = response.json()
    if not data.get("success"):
        raise RuntimeError(data.get("message"))

    return data["message"]


@task
def daily_brand_popularity_index_task():
    return trigger_spark_job("daily_brand_popularity_index")


@flow(name="daily_brand_popularity_index")
def daily_brand_popularity_index():
    result = daily_brand_popularity_index_task.submit()
    print(result.result())


if __name__ == "__main__":
    daily_brand_popularity_index()
