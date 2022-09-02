import json
import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

os.environ['AIRFLOW_CONN_HTTP'] = "http://api.github.com/https"

with DAG(schedule_interval=None, start_date=datetime(1970, 1, 1), dag_id="outline_dag") as dag:
    echo_operator = BashOperator(
        task_id="echo",
        bash_command="date"
    )

    query_github_stats = SimpleHttpOperator(
        task_id="http",
        endpoint="repos/apache/airflow",
        method="GET",
        http_conn_id="http",
        log_response=True
    )


    @task()
    def extract_fork_count(date, github_stats: str):
        github_stats_json = json.loads(github_stats)
        airflow_stars = github_stats_json.get("stargazers_count")
        print(f"As of {date}, Apache Airflow has {airflow_stars} stars on Github!")


    echo_operator >> query_github_stats
    extract_fork_count(echo_operator.output, query_github_stats.output)
