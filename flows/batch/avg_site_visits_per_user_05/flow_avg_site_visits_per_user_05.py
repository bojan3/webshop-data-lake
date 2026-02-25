import argparse

import requests
from prefect import flow, task


def trigger_spark_job(job_name: str, data_period: str):
    filename = f"batch/avg_site_visits_per_user_05/{job_name}.py"

    response = requests.post(
        "http://spark-master:8000/run", params={"filename": filename, "data_period": data_period}
    )

    response.raise_for_status()

    data = response.json()

    if not data.get("success"):
        raise RuntimeError(data.get("message"))

    return data["message"]


@task
def load_and_store_monthly_site_visits_task(data_period: str):
    return trigger_spark_job("load_and_store_monthly_site_visits", data_period)


@task
def count_site_visits_by_user_task(data_period: str):
    return trigger_spark_job("count_site_visits_by_user", data_period)


@task
def calculate_avg_site_visits_per_user_task(data_period: str):
    return trigger_spark_job("calculate_avg_site_visits_per_user", data_period)


@task
def publish_avg_site_visits_per_user_task(data_period: str):
    return trigger_spark_job("publish_avg_site_visits_per_user", data_period)


@flow(name="avg_site_visits_per_user_05")
def avg_site_visits_per_user_05(data_period: str):
    load_msg = load_and_store_monthly_site_visits_task.submit(data_period)
    count_msg = count_site_visits_by_user_task.submit(data_period, wait_for=[load_msg])
    calculate_msg = calculate_avg_site_visits_per_user_task.submit(
        data_period, wait_for=[count_msg]
    )
    publish_msg = publish_avg_site_visits_per_user_task.submit(
        data_period, wait_for=[calculate_msg]
    )

    print(
        load_msg.result()
        + count_msg.result()
        + calculate_msg.result()
        + publish_msg.result()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-period", "--data_period", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    avg_site_visits_per_user_05(args.data_period)
