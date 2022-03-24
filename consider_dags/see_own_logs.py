from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.utils.log.log_reader import TaskLogReader


def get_logs(try_number=1, **context):

    # get the task instances for the other tasks in this dag
    dagrun = context["dag_run"]
    task_instances = dagrun.get_task_instances()
    hello_task_instance = next(filter(lambda ti: ti.task_id == "hello", task_instances))
    world_task_instance = next(filter(lambda ti: ti.task_id == "world", task_instances))

    # read their logs
    def check(ti, expect, notexpect):
        task_log_reader = TaskLogReader()
        log_container, _ = task_log_reader.read_log_chunks(
            ti, try_number, {"download_logs": True}
        )
        logs = log_container[0][0][1]
        print(f"Found logs: '''{logs}'''")
        assert expect in logs
        assert notexpect not in logs
        print(f"Found '''{expect}''' but not '''{notexpect}'''")

    # make sure expected output appeared
    check(hello_task_instance, "INFO - Done. Returned value was: Hello", "World")
    check(world_task_instance, "INFO - Done. Returned value was: World", "Hello")


@dag(schedule_interval=None, start_date=days_ago(2), tags=['core'],)
def see_own_logs():
    """
    The last task will fail if logs are mishandled.
    For instance: https://github.com/apache/airflow/issues/19058
    """
    (
        PythonOperator(task_id="hello", python_callable=lambda: "Hello")
        >> PythonOperator(task_id="world", python_callable=lambda: "World")
        >> PythonOperator(task_id="get_logs", python_callable=get_logs)
    )


the_dag = see_own_logs()
