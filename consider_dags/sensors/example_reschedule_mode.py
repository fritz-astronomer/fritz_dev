from airflow import DAG
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.dates import days_ago

docs = """
####Purpose
This dag tests that tasks can acheive the state of 'reschedule'.
####Expected Behavior
This dag has one task that is will stay stuck in the 'reschedule' state. 
"""

class DummySensor(BaseSensorOperator):
    def poke(self, context):
        return False


with DAG(
    "reschedule_mode",
    start_date=days_ago(1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    doc_md=docs,
    tags=["core", "sensor"],
) as dag3:
    DummySensor(task_id="wait-task", poke_interval=60 * 5, mode="reschedule")
