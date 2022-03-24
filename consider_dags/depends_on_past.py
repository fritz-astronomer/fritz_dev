from airflow import models
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta

from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag_name = "depends_on_past"

with models.DAG(
    dag_name,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=5,
    tags=["core"],
) as dag:

    test1 = DummyOperator(
        task_id="should_pass_1",
        task_concurrency=10,
    )

    test2 = BashOperator(
        task_id="should_pass_2",
        bash_command="echo hi",
        depends_on_past=True,
    )

    test3 = BashOperator(
        task_id="should_pass_3",
        bash_command="echo hi",
    )

    test1 >> test2 >> test3
