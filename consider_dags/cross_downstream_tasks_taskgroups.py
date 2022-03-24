from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label
from airflow.models.baseoperator import cross_downstream, chain
from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models.dag import DAG

from datetime import datetime, timedelta

two_days = datetime.now() - timedelta(days=2)

default_args = {
    'owner':'airflow',
    'depends_on_past': True
}

@task
def t0():
    return "This is a decorated task"

with DAG(
    dag_id="cross_downstream_tasks_and_task_groups",
    schedule_interval=None,
    start_date=two_days,
    default_args=default_args,
    tags=['core'],
) as dag:
    """
    t0 = BashOperator(
        task_id="sleep_3_seconds",
        bash_command="sleep 3"
    )"""

    with TaskGroup(group_id="group1") as tg1:
        t1 = DummyOperator(task_id="dummy1")
        t2 = DummyOperator(task_id="dummy2")
        t3 = DummyOperator(task_id="dummy3")
    
    t7 = BashOperator(
        task_id="bash_echo",
        bash_command="echo continue.."
    )

    t8 = DummyOperator(task_id="dummy4")
    t9 = DummyOperator(task_id="dummy5")
    t10 = DummyOperator(task_id="dummy6")
    t11 = DummyOperator(task_id="dummy7")
    t12 = DummyOperator(task_id="dummy8")

#using taskgroups as from_tasks, using regular tasks as to_tasks
#Label doesn't work with cross_downstream
cross_downstream(from_tasks=[tg1, t7], to_tasks=[t0(), t8, t9, t10, t11, t12])