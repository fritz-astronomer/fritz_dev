from datetime import datetime

from airflow.configuration import conf
from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


@dag(
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False
)
def example_kpo():
    KubernetesPodOperator(
        kubernetes_conn_id="kubernetes",
        namespace=conf.get('kubernetes', 'NAMESPACE'),
        log_events_on_failure=True,
        task_id="kpo-test",
        name="kpo-test",
        image="ubuntu",
        cmds=["echo"],
        arguments=["hi"]
    )


example_kpo_dag = example_kpo()
