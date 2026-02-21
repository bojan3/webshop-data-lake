import requests
from prefect import flow, task


def trigger_spark_job(job_name: str):
    filename = f"batch/avg_events_per_session_by_event_type_06/{job_name}.py"

    response = requests.post(
        "http://spark-master:8000/run",
        params={"filename": filename},
    )

    response.raise_for_status()

    data = response.json()

    if not data.get("success"):
        raise RuntimeError(data.get("message"))

    return data["message"]


@task
def load_and_store_monthly_events_task():
    return trigger_spark_job("load_and_store_monthly_events")


@task
def count_events_by_session_and_event_type_task():
    return trigger_spark_job("count_events_by_session_and_event_type")


@task
def calculate_average_events_per_session_by_event_type_task():
    return trigger_spark_job("calculate_average_events_per_session_by_event_type")


@flow(name="avg_events_per_session_by_event_type_06")
def avg_events_per_session_by_event_type_06():
    msg1 = load_and_store_monthly_events_task()
    msg2 = count_events_by_session_and_event_type_task()
    msg3 = calculate_average_events_per_session_by_event_type_task()
    print(msg1 + msg2 + msg3)


if __name__ == "__main__":
    avg_events_per_session_by_event_type_06()
