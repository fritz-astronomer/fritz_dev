from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

from pprint import pprint

docs = """
####Purpose
The purpose of this dag is to test the 'prefix_group_id' keyword arg of the TaskGroup task organizer.\n
It achieves testing this kwarg by making an assertion that tasks in the format: 'dag_id.task_id' are in the task instances.\n
If the kwarg 'prefix_group_id' was set to true the task id's would be in the format of: 'dag_id.group_id.task_id' .
####Expected Behavior
This dag has 14 dummy tasks and one python operator task that checks that the TaskGroup isn't applying group id's to the task id.\n
If the last task fails there is something wrong with the TaskGroup kwarg 'prefix_group_id'.\n
If the first 14 tasks fail then there is something fundamentally wrong with the TaskGroup.
"""


def get_the_tis(**context):
    dagrun = context["dag_run"]
    task_instances = dagrun.get_task_instances()

    # change the task instance values to string datatypes
    str_ls = [str(i) for i in task_instances]
    for j in str_ls:
        if "taskgrp_prefix.check_no_group_id_prefix" in j:
            continue
        else:
            # if the 'prefix_group_id was set to True then the task id would be:
            # 'dag_id.group_id.task_id'
            # but since it's set to false it follows the format of:
            # 'dag_id.task_id'
            assert "taskgrp_prefix.dummy" in j
            print("The assertion has passed.")


with DAG(
    dag_id="taskgrp_prefix",
    start_date=days_ago(2),
    schedule_interval=None,
    doc_md=docs,
    tags=["core", "taskgroups"],
) as dag:

    with TaskGroup(group_id="group1", prefix_group_id=False) as group1:
        d0 = DummyOperator(task_id="dummy0")
        d1 = DummyOperator(task_id="dummy1")
        d0 >> d1
        for i in range(2, 7):
            d1 >> DummyOperator(task_id=f"dummy{i}")

    with TaskGroup(group_id="group2", prefix_group_id=False) as group2:
        d0 = DummyOperator(task_id="dummy8")
        d1 = DummyOperator(task_id="dummy9")
        d0 >> d1
        for i in range(10, 15):
            d1 >> DummyOperator(task_id=f"dummy{i}")

    py16 = PythonOperator(
        task_id="check_no_group_id_prefix",
        python_callable=get_the_tis,
    )

[group1, group2] >> py16
