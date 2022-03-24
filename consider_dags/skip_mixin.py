from airflow.models import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="skip_mixin_task",
    default_args={"owner": "airflow", "start_date": days_ago(2)},
    schedule_interval=None,
    tags=["core"],
) as dag:

    def needs_some_extra_task(some_bool_field, **kwargs):
        if some_bool_field:
            return f"extra_task"
        else:
            return f"final_task"

    branch_op = BranchPythonOperator(
        task_id=f"branch_task",
        provide_context=True,
        python_callable=needs_some_extra_task,
        op_kwargs={"some_bool_field": True},  # For purposes of showing the problem
    )

    # will be always ran in this example
    extra_op = DummyOperator(
        task_id=f"extra_task",
    )
    extra_op.set_upstream(branch_op)

    # should not be skipped
    final_op = DummyOperator(
        task_id="final_task",
        trigger_rule="none_failed_or_skipped",
    )
    final_op.set_upstream([extra_op, branch_op])
