from airflow.operators.python import task
from airflow.models import DAG
from airflow.utils.dates import days_ago


@task
def people():
    return [{"name": "Mark", "age": 10}, {"name": "Jack", "age": 11}]


@task
def list_people(people):
    for item in people:
        print(item["name"], "is", item["age"], "years old")


with DAG(
    "example_at_task_decorator",
    default_args={"owner": "airflow"},
    start_date=days_ago(2),
    schedule_interval=None,
    tags=['core'],
) as dag:
    task1 = people()
    list_people(task1)
