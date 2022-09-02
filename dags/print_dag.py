import os
from datetime import datetime

from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import DAG

os.environ['AIRFLOW_CONN_TEST'] = "postgresql://user:pass@host:1234"


with DAG(dag_id="print_dag", schedule_interval=None, start_date=datetime(1970, 1, 1)) as dag:
    @task()
    def get_connection():
        conn = BaseHook.get_connection(conn_id="TEST")
        print(conn.conn_type, conn)
        print(conn.get_uri())

    get_connection()