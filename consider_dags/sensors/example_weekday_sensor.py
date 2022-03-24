from socket import timeout
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.sensors.weekday_sensor import DayOfWeekSensor
from airflow.sensors.base import BaseSensorOperator


with DAG(
    "example_week_sensor",
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["core", "sensor"],
) as dag:
    working_day_check = DayOfWeekSensor(
        task_id="working_day_check",
        # added weekend days so that no matter what any day of the week this sensor will work.
        week_day={"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"},
        use_task_execution_day=True,
        poke_interval=20.0,
        timeout=120.0,
        dag=dag,
    )
