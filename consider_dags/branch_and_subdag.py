import random
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def return_branch():
    branches = ["subdag_0", "subdag_1", "subdag_2", "subdag_3", "subdag_4"]
    # this will choose a random branch to follow.
    return random.choice(branches)


def load_subdag(parent_dag_name, child_dag_name):
    with DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=default_args,
        schedule_interval=None,
    ) as dag_subdag:
        for i in range(5):
            # sleep randomly between 0-10s then print the date
            t = BashOperator(
                bash_command="sleep [ ( $RANDOM % 10 )  + 1 ]s  date",
                task_id=f"load_subdag_{i}",
                dag=dag_subdag,
                pool="default_pool",
            )
    return dag_subdag


with DAG(
    "branch_and_subdag",
    start_date=days_ago(1),
    max_active_runs=3,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,  # enable if you don't want historical dag runs to run
    tags=["core"],
) as dag:
    kick_off_dag = DummyOperator(task_id="run_this_first")

    branching = BranchPythonOperator(task_id="branching", python_callable=return_branch)

    kick_off_dag >> branching

    for i in range(0, 5):
        d = SubDagOperator(
            task_id=f"subdag_{i}",
            subdag=load_subdag("branch_and_subdag", f"subdag_{i}"),
            conf={"ab": "bc"},
        )

        branching >> d
