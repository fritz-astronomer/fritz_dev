from typing import List, Optional

from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import models as k8s, V1ResourceRequirements


class CustomKubernetesPodOperator(KubernetesPodOperator):
    # Note - remove this, they are already set in the parent class
    # template_fields = ["env_vars", "image"]

    def __init__(
        self,
        model: str,
        name: str,
        namespace: Optional[str] = None,
        external_conn_id: Optional[str] = None,
        env_vars: Optional[List[k8s.V1EnvVar]] = None,
        image: str = "{{ var.value.DBT_IMAGE }}",
        run_children: Optional[bool] = True,
        resources: Optional[V1ResourceRequirements] = None,
        secret_deploy_target: str = "DBT_DB_PASSWORD",
        secret_secret: str = "airflow-dbt-settings",
        secret_key: str = "DBT_DB_PASSWORD",
        **kwargs,
    ):
        cmd_run = f"dbt run -m {model}{'+' if run_children else ''}"
        super().__init__(
            config_file="/usr/local/airflow/include/.kube/config",
            env_vars=env_vars or {
                "user": "{{ conn." + external_conn_id + ".login }}",
                "host": "{{ conn." + external_conn_id + ".host }}",
                'port': "{{ conn." + external_conn_id + ".port }}",
                "name": "{{ conn." + external_conn_id + ".schema }}",
                "schema": "{{ conn." + external_conn_id + ".schema }}",
            },
            name=name,
            resources=resources or V1ResourceRequirements(
                limits={
                    "cpu": "1500m",
                    "memory": "2Gi",
                },
                requests={
                    "cpu": "400m",
                    "memory": "500Mi",
                }
            ),
            startup_timeout_seconds=300,
            arguments=[cmd_run],
            cmds=["bash", "-cx"],
            get_logs=True,
            secrets=[Secret(
                deploy_type="env",
                deploy_target=secret_deploy_target,
                secret=secret_secret,
                key=secret_key,
            )],
            verify_ssl=False,
            image=image,
            namespace=namespace,
            in_cluster=False,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
            **kwargs,
        )
