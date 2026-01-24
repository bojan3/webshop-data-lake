from prefect import flow, task
import subprocess

@task(retries=1, retry_delay_seconds=30)

def run_spark_job():
    subprocess.run(
        [
            "docker", "exec", "spark-master",
            "/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077",
            "/flows/first_batch_job.py"
        ],
        check=True
    )

@flow(name="events-apr-2020-batch")
def events_batch_flow():
    run_spark_job()

if __name__ == "__main__":
    events_batch_flow()
