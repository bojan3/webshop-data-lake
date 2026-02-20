import requests
from prefect import flow, task


def trigger_spark_job(job_name: str):
    filename = f"{job_name}.py"

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
def count_brand_events():
    return trigger_spark_job("brand_event_count")

@task
def sum_brand_events(msg: str):
    return trigger_spark_job("brand_event_count_merge")

@task
def compute_brand_points(msg: str):
    return trigger_spark_job("calculate_brand_points")

@flow(name="calculate_brand_points")
def calculate_brand_points():
    msg1 = count_brand_events()
    msg2 = sum_brand_events(msg1)
    msg3 = compute_brand_points(msg2)
    print(msg1 + msg2 + msg3)


if __name__ == "__main__":
    calculate_brand_points()

