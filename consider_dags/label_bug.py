from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.models.dag import DAG

from datetime import datetime, timedelta

two_days = datetime.now() - timedelta(days=2)

docs = """
####Purpose
This dag is designed to show that when applying the edge_modifer=Label to a taskgroup that a cycle is produced in the graph view of the UI.
####Expected Behavior
This dags tasks are all expected to succeed as taskgroups solely interact with the UI and thus do not interact with how airflow works internally.
"""

with DAG(
    dag_id="label_bug",
    schedule_interval=None,
    start_date=two_days,
    doc_md=docs,
    tags=['core'],
) as dag:

    with TaskGroup(group_id="group1") as taskgroup1:
        t1 = DummyOperator(task_id="dummmy1")
        t2 = DummyOperator(task_id="dummy2")
        t3 = DummyOperator(task_id="dummy3")
    
    t4 = DummyOperator(task_id="dummy4")

chain([Label("branching to group tasks"), Label("stuff")], taskgroup1, t4,)
