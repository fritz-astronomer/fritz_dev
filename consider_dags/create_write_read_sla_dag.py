from datetime import timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.log.logging_mixin import LoggingMixin

def write_file():
    try:

        with open("/usr/local/airflow/test.txt", "wt") as fout:
            for i in range(10):
                fout.write("This is line %d\r\n" % (i + 1))

    except Exception as e:
        LoggingMixin().log.error(e)
        raise AirflowException(e)


def read_file():
    try:

        with open("/usr/local/airflow/test.txt") as fout:
            contents = fout.read()
            print(contents)

    except Exception as e:
        LoggingMixin().log.error(e)
        raise AirflowException(e)


# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    #    'retries': 1,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(seconds=6),
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG(
    "write_read_sla",
    start_date=days_ago(2),
    max_active_runs=3,
    schedule_interval="@daily",  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
    default_args=default_args,
    catchup=True,  # enable if you want historical dag runs to run
    tags=["core"],
) as dag:
    t0 = DummyOperator(task_id="start")
    # create task
    t1 = BashOperator(
        task_id="create_file",
        bash_command="cd /usr/local/airflow && touch test.txt",
        dag=dag,
    )

    # sla test case
    t2 = BashOperator(
        task_id="Should_send_sla_email",
        bash_command="sleep 10",
        dag=dag,
    )

    # write task
    t3 = PythonOperator(
        task_id="write_file_task",
        python_callable=write_file,  # make sure you don't include the () of the function
        dag=dag,
    )

    # Read task
    t4 = PythonOperator(
        task_id="read_file_task",
        python_callable=read_file,  # make sure you don't include the () of the function
        dag=dag,
    )

    t0 >> t1 >> t2 >> t3 >> t4
