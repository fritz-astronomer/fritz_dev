from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.time_sensor import TimeSensor
from datetime import datetime, timedelta

now = datetime.now() + timedelta(minutes=3)


with DAG(
    "example_time_sensor",
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["core", "sensor"],
) as dag:
    task = TimeSensor(task_id="wait_1", target_time=now.time())
