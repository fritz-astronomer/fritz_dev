from datetime import datetime
from pathlib import Path

from airflow.configuration import conf
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.executors.kubernetes_executor import KubeConfig, create_pod_id
from airflow.kubernetes.pod_generator import PodGenerator
from airflow.models import DagRun, TaskInstance, DAG, BaseOperator
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.settings import pod_mutation_hook
from airflow.utils import trigger_rule
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8s


def convert_executor_config_kube_exec_to_celery_kpo(task: BaseOperator):
    dr = DagRun(task.dag.dag_id, execution_date=datetime(1970, 1, 1))
    kube_config = KubeConfig()
    ti = TaskInstance(task, None)
    ti.dag_run = dr
    pod = PodGenerator.construct_pod(
        dag_id=task.dag.dag_id,
        task_id=ti.task_id,
        pod_id=create_pod_id(task.dag.dag_id, ti.task_id),
        try_number=ti.try_number,
        kube_image=kube_config.kube_image,
        date=ti.execution_date,
        args=ti.command_as_list(),
        pod_override_object=PodGenerator.from_obj(ti.executor_config),
        scheduler_job_id="worker-config",
        namespace=kube_config.executor_namespace,
        base_worker_pod=PodGenerator.deserialize_model_file(kube_config.pod_template_file) if Path(
            kube_config.pod_template_file).is_file() else k8s.V1Pod(),
    )
    pod_mutation_hook(pod)
    return pod


with DAG(
    dag_id="kube_exec_to_celery_kpo_dag",
    schedule_interval="@daily",
    catchup=False,
    start_date=datetime(2022, 1, 1),
    default_args={}
) as dag:
    compute_resources = k8s.V1ResourceRequirements(
        limits={"cpu": "800m", "memory": "3Gi"},
        requests={"cpu": "800m", "memory": "3Gi"}
    )

    @task
    def skip():
        raise AirflowSkipException()
    skip_task = skip()

    bo = BashOperator(
        task_id="bash_operator",
        bash_command="echo hi",
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            resources=compute_resources
                        )
                    ]
                )
            )
        },
        trigger_rule=TriggerRule.NONE_SKIPPED
    )
    skip_task >> bo

    full_pod_spec = convert_executor_config_kube_exec_to_celery_kpo(task=bo, )

    kpo = KubernetesPodOperator(
        namespace=conf.get("kubernetes", "NAMESPACE"),
        task_id="bash_operator_via_kube_pod_operator",
        cmds=["python"],
        arguments=[
            "-c",
            '''"from airflow.operators.bash import BashOperator; from airflow.utils.context import Context; BashOperator(task_id="bash_operator", bash_command="echo hi").execute({})"'''
        ],
        full_pod_spec=full_pod_spec,
        resources=compute_resources,
        is_delete_operator_pod=True,
        in_cluster=True,
        get_logs=True,
    )


    @task
    def print_task():
        print(conf.as_dict(display_sensitive=True))
        print(full_pod_spec)

    print_task()

