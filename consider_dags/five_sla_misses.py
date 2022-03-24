from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent
from time import sleep

decade = 31556952 * 10

with DAG(
    "five_sla_misses",
    start_date=datetime(1970, 1, 1),
    schedule_interval=timedelta(seconds=decade),
    catchup=True,
    doc_md=dedent(
        """
        # Purpose

        Generate an expected number of SLA misses

        ## To Test

        1. Unpause the dag
        2. Wait for dagruns to complete
        3. [browse] -> [SLA Misses]

        ## Expect

        five dagruns, and five SLA misses:

        - 1970 - 1980
        - 1980 - 1990
        - 1990 - 2000
        - 2000 - 2010
        - 2010 - 2020
        """
    ),
) as miss:
    PythonOperator(
        task_id="miss",
        python_callable=lambda x: sleep(x),
        op_args=[20],
        sla=timedelta(seconds=15),
    )
