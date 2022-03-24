from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.utils.dates import days_ago

dag = DAG(
    dag_id="latest_only",
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["core"],
)

latest_only = LatestOnlyOperator(task_id="latest_only", dag=dag)
task1 = DummyOperator(task_id="task1", dag=dag)

latest_only >> task1
