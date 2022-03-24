from datetime import timedelta

from airflow import DAG
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import days_ago

with DAG(
    "example_timedelta_sensor",
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["core", "sensor"],
) as dag:
    task = TimeDeltaSensor(task_id="wait_1", delta=-timedelta(days=1))
