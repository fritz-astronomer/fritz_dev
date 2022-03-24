from airflow.decorators import dag, task
from airflow.utils.dates import days_ago


def create_dag(dag_id):
    @dag(
        default_args={"owner": "airflow", "start_date": days_ago(1)},
        schedule_interval=None,
        dag_id=dag_id,
        catchup=False,
        is_paused_upon_creation=False,
        tags=["core"],
    )
    def dynamic_dag():
        @task()
        def dynamic_task_1(*name):
            return 1

        task_1 = dynamic_task_1()
        task_2 = dynamic_task_1(task_1)

    return dynamic_dag()


for i in range(3):
    dag_id = f"dynamic_dag_{i}"
    globals()[dag_id] = create_dag(dag_id)
