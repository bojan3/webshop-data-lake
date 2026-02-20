from prefect import flow, task

# this file is just flow code

@task
def create_message():
    msg = "Hello from Prefect"
    return(msg)

@flow
def hello_world():
    task_message = create_message()
    print(task_message)

hello_world()
