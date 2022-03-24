from airflow.models import DAG, Connection
from airflow import settings
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.filesystem import FSHook
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException

dag_name = "example_filesystem_sensor"

def add_conn():
    try:
        found = BaseHook().get_connection(f"{dag_name}_connection")
    except AirflowNotFoundException:
        found = None
        print("The connection is not defined, please add a connection in the dags first task")
    if found:
        print("The connection has been made previously.")
    else:
        local_connection = Connection(
            conn_id=f"{dag_name}_connection",
            conn_type="File",
            host="/tmp/fakefile"
            )
        print(local_connection)
        session = settings.Session()
        session.add(local_connection)
        session.commit()
 


with DAG(
    dag_id=dag_name,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["core", "sensor"],
) as dag:

    task0 = PythonOperator(
        task_id="add_connection",
        python_callable=add_conn
    )

    task1 = BashOperator(
        task_id="create_file",
        bash_command="mkdir -p /tmp/fakefile && touch /tmp/fakefile/restart.txt",
    )
    task2 = FileSensor(
        task_id="check_file",
        filepath="/tmp/fakefile/restart.txt",
        fs_conn_id=f"{dag_name}_connection",
        poke_interval=20.0,
        timeout=180.0,
        )

    task3 = BashOperator(task_id="cleanup", bash_command="rm -r /tmp/fakefile")

    task0 >> task1 >> task2 >> task3
