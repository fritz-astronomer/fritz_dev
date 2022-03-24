from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.bash import BashSensor

date = "{{ ds }}"

with DAG(
    "example_bash_sensor",
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["core", "sensor"],
) as dag:
    task1 = BashSensor(task_id="sleep_10", bash_command="sleep 10")
    task2 = BashSensor(
        task_id="sleep_total",
        bash_command="echo $EXECUTION_DATE",
        env={"EXECUTION_DATE": date},
    )
