from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

from pprint import pprint

docs = """
####Purpose
The purpose of this dag is to test the 'add_suffix_on_collision' keyword arg of the TaskGroup task organizer.\n
It achieves testing this kwarg by making an assertion that task id's for group 2 have a suffix of __1 added to the end of the group id.\n
####Expected Behavior
This dag has 14 dummy tasks and one python operator task that checks that the TaskGroup is applying group id suffixes to the task id.\n
If the last task fails there is something wrong with the TaskGroup kwarg 'add_suffix_on_collision'.\n
If the first 14 tasks fail then there is something fundamentally wrong with the TaskGroup.
"""


def get_the_tis(**context):
    dagrun = context["dag_run"]
    task_instances = dagrun.get_task_instances()

    # change the task instance values to string datatypes
    str_ls = [str(i) for i in task_instances]
    for j in str_ls:
        # filters task instances for task id's that have suffixes added to them
        if "taskgrp_add_suffix_on_collision.group1__" in j:
            # makes an assertion that the taskgroup is adding suffixes to duplicate groupd ID's
            assert "taskgrp_add_suffix_on_collision.group1__" in j


with DAG(
    dag_id="taskgrp_add_suffix_on_collision",
    start_date=days_ago(2),
    schedule_interval=None,
    doc_md=docs,
    tags=["core", "taskgroups"],
) as dag:

    with TaskGroup(group_id="group1", add_suffix_on_collision=True) as group1:
        d0 = DummyOperator(task_id="dummy0")
        d1 = DummyOperator(task_id="dummy1")
        d0 >> d1
        for i in range(2, 7):
            d1 >> DummyOperator(task_id=f"dummy{i}")

    with TaskGroup(group_id="group1", add_suffix_on_collision=True) as group2:
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
