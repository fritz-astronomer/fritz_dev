from airflow.models import DAG, Connection
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowNotFoundException
from airflow import settings

from pprint import pprint

dag_name = "simple_http_operator"

def add_conn():
    try:
        found = BaseHook().get_connection(f"{dag_name}_connection")
    except AirflowNotFoundException:
        found = None
        print("The connection is not defined, please add a connection in the dags first task")
    if found:
        print("The connection has been made previously.")
    else:
        remote_connection = Connection(
            conn_id=f"{dag_name}_connection",
            conn_type=HttpHook().conn_type,
            host="www.astronomer.io",
            schema="https",
            port=443,
            )
        print(remote_connection)
        session = settings.Session()
        session.add(remote_connection)
        session.commit()
    

def print_the_response(response):    
    if "DMCA" in response.text:
        pprint(response.text)
        return True
    else:
        pprint("The specified string is not in the response, please check your spelling.")
        return False
        

with DAG(
    dag_id=dag_name,
    start_date=days_ago(2),
    schedule_interval=None,
) as dag:

    py0 = PythonOperator(
        task_id="add_connection",
        python_callable=add_conn
    )

    h1 = SimpleHttpOperator(
        task_id="simple_http_operator",
        method="GET",
        http_conn_id=f"{dag_name}_connection",
        endpoint="terms",
        response_check=print_the_response,
    )

py0 >> h1
