import requests
from prefect import flow, task


@task
def trigger_spark_job():
    response = requests.post(
        "http://spark-master:8000/run", params={"filename": "first_batch_job.py"}
    )

    response.raise_for_status()

    data = response.json()

    if not data.get("success"):
        raise RuntimeError(data.get("message"))

    return data["message"]


@flow(name="events-apr-2020-batch")
def events_batch_flow():
    msg = trigger_spark_job()
    print(msg)


if __name__ == "__main__":
    events_batch_flow()
