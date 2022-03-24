"""Example DAG demonstrating the usage of the ShortCircuitOperator."""

# testing to pass with True/False and Integer as return value
from random import randint

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils import dates

args = {
    "owner": "airflow",
}

dag = DAG(
    dag_id="example_short_circuit_operator",
    default_args=args,
    start_date=dates.days_ago(2),
    tags=["core"],
)

cond_true = ShortCircuitOperator(
    task_id="condition_is_True",
    python_callable=lambda: True,
    dag=dag,
)

cond_false = ShortCircuitOperator(
    task_id="condition_is_False",
    python_callable=lambda: False,
    dag=dag,
)


def shortcircuit_fn():
    return randint(0, 10) == 8


short = ShortCircuitOperator(
    dag=dag, task_id="short_circuit", python_callable=shortcircuit_fn
)
task_1 = DummyOperator(dag=dag, task_id="task_1")
task_2 = DummyOperator(dag=dag, task_id="task_2")

task_A = [DummyOperator(task_id="true_" + str(i), dag=dag) for i in [1, 2]]
task_B = [DummyOperator(task_id="false_" + str(i), dag=dag) for i in [1, 2]]

cond_true >> task_A
cond_false >> task_B
short >> task_1 >> task_2
