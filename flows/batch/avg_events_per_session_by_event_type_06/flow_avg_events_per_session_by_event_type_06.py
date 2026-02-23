import argparse

import requests
from prefect import flow, task


def trigger_spark_job(job_name: str, data_period: str):
    filename = f"batch/avg_events_per_session_by_event_type_06/{job_name}.py"

    response = requests.post(
        "http://spark-master:8000/run",
        params={"filename": filename, "data_period": data_period},
    )

    response.raise_for_status()

    data = response.json()

    if not data.get("success"):
        raise RuntimeError(data.get("message"))

    return data["message"]


@task
def load_and_store_monthly_events_task(data_period: str):
    return trigger_spark_job("load_and_store_monthly_events", data_period)


@task
def count_events_by_session_and_event_type_task(data_period: str):
    return trigger_spark_job("count_events_by_session_and_event_type", data_period)


@task
def calculate_average_events_per_session_by_event_type_task(data_period: str):
    return trigger_spark_job(
        "calculate_average_events_per_session_by_event_type", data_period
    )


@flow(name="avg_events_per_session_by_event_type_06")
def avg_events_per_session_by_event_type_06(data_period: str):
    msg1 = load_and_store_monthly_events_task.submit(data_period)
    msg2 = count_events_by_session_and_event_type_task.submit(data_period, wait_for=[msg1])
    msg3 = calculate_average_events_per_session_by_event_type_task.submit(
        data_period, wait_for=[msg2]
    )
    print(msg1.result() + msg2.result() + msg3.result())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    avg_events_per_session_by_event_type_06(args.data_period)
