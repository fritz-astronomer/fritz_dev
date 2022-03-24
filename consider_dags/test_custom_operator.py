from airflow import models
from airflow.utils.dates import days_ago
from config.custom_operator import HelloOperator

default_args = {"owner": "airflow", "start_date": days_ago(1)}

dag_name = "test_custom_operator"

with models.DAG(
    dag_name,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=5,
    tags=["core"],
) as dag:
    test1 = HelloOperator(task_id="should_pass_1", name="foo_bar")
