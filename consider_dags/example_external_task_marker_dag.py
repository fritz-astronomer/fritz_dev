from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.utils.dates import days_ago

start_date = days_ago(1)

with DAG(
    dag_id="example_external_task_marker_parent",
    start_date=start_date,
    schedule_interval=None,
    tags=["core"],
) as parent_dag:
    # [START howto_operator_external_task_marker]
    parent_task = ExternalTaskMarker(
        task_id="parent_task",
        external_dag_id="example_external_task_marker_child",
        external_task_id="child_task1",
    )
    # [END howto_operator_external_task_marker]

with DAG(
    dag_id="example_external_task_marker_child",
    start_date=start_date,
    schedule_interval=None,
    tags=["core"],
) as child_dag:
    # [START howto_operator_external_task_sensor]
    child_task1 = ExternalTaskSensor(
        task_id="child_task1",
        external_dag_id=parent_dag.dag_id,
        external_task_id=parent_task.task_id,
        timeout=600,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
    )
    # [END howto_operator_external_task_sensor]
    child_task2 = DummyOperator(task_id="child_task2")
    child_task1 >> child_task2
