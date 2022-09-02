import os

from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

# ### <SETUP> ####
# $ cat requirements.txt
# apache-airflow-providers-cncf-kubernetes==4.1.0
# 
# https://www.journaldev.com/52730/configuring-service-account-tokens-kubeconfig
# 
# touch ~/.kube/kpo-test
# kubectl create namespace kpo-test
# kubectl create serviceaccount --namespace kpo-test kpo-test
# kubectl create role kpo-test --namespace kpo-test --verb=get --verb=list --verb=patch --verb=delete --verb=create --verb=watch --resource=pods --resource=pods/log
# kubectl create rolebinding --serviceaccount=kpo-test:kpo-test --role=kpo-test --namespace=kpo-test kpo-test
# kubectl -n kpo-test get serviceaccount/kpo-test -o jsonpath='{.secrets[0].name}'
# SECRET=$(kubectl -n kpo-test get serviceaccount/kpo-test -o jsonpath='{.secrets[0].name}')
# TOKEN=$(kubectl -n kpo-test get secret $SECRET -o jsonpath='{.data.token}' | base64 --decode)
# kubectl --kubeconfig="/Users/mac/.kube/kpo-test" config set-credentials kpo-test --token=$TOKEN
# kubectl --kubeconfig="/Users/mac/.kube/kpo-test" config set-cluster polaris --server=https://4FB5BA04C75599220274BBB942AE2DEA.gr7.us-east-1.eks.amazonaws.com
# kubectl --kubeconfig="/Users/mac/.kube/kpo-test" config set-context kpo-test --cluster=polaris --user=kpo-test --namespace=kpo-test
# kubectl --kubeconfig="/Users/mac/.kube/kpo-test" config set-context --current --user=kpo-test
# cat ~/.kube/config | python -c "import sys, urllib.parse; print(urllib.parse.quote(sys.stdin.read()))"
# os.environ["AIRFLOW_CONN_KPO"] = "kubernetes://?extra__kubernetes__kube_config=%7B%22apiVersion%22%3A%22v1%22%2C%22clusters%22%3A%5B%7B%22cluster%22%3A%7B%22certificate-authority-data%22%3A%22LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUM1ekNDQWMrZ0F3SUJBZ0lCQURBTkJna3Foa2lHOXcwQkFRc0ZBREFWTVJNd0VRWURWUVFERXdwcmRXSmwKY201bGRHVnpNQjRYRFRJeE1EVXhNVEl5TURZeU5Wb1hEVE14TURVd09USXlNRFl5TlZvd0ZURVRNQkVHQTFVRQpBeE1LYTNWaVpYSnVaWFJsY3pDQ0FTSXdEUVlKS29aSWh2Y05BUUVCQlFBRGdnRVBBRENDQVFvQ2dnRUJBSzBiCnpKaEM2bW5oMnptWTg0bGtqR0xoM3JaZUdVMmdXRjVNR3E3TUZncEZEYmN2V09Xa3pXTFNrTmRmR3BnV1B2bHkKdEs0RUwxdWZmRHpXeWJmUSsrYlAxZmxoRkVvUVV1U1ZyODBXUGdLN2wzb1NzejB3bEY1WStxOU1vempPejg5OApTa3kwbzNQWmhvS1JreUVZM0l1K2dVMlZIYkNRTDZSME9zc0N6Uk9uSHppdnhjcmpqcVFTTUhYQWlmdGVseXIwCnZTKzV4WlMxRDYrb3JNeEVEVk5KUTVaejl4c0Q0eG9wQUhHR3lhTzBRYnZvdmdVcTFDR3pody92MDd1WGR5M3kKNGZjeGhEUkNVNVNia0lSTWVKcjhZck92YzVuWk54MEFMdC9ZNk1WRTRyN0hSeVRSeGU1ajJWQUR4bnJscTgyTQplQmc4b2xIeFJTdWF5SUF6RjljQ0F3RUFBYU5DTUVBd0RnWURWUjBQQVFIL0JBUURBZ0trTUE4R0ExVWRFd0VCCi93UUZNQU1CQWY4d0hRWURWUjBPQkJZRUZBMk5IT0JUZXd6ZlBmNkRPQmJOYTVtRks4M2RNQTBHQ1NxR1NJYjMKRFFFQkN3VUFBNElCQVFCU3BHeUt2bGZ1V1dqQ1ZMdE1OcXJOcERKMytYdU1KN0VpNVJDaC9wUzJSQWRIQjNucwplUmRkcmdFYm4zMHpyaWx2MkhQME1nMDlaZjR1b2ZsWGEvbVA4OWJkdjUyb3VML3MxZ2VVVnR2c1Y3MlF5OUlOCkRZblF4K1NwL1JIK3RvcnMzc3lnc3NUOEZkWnU3QUtqTitRNW1kQUlIZGFCeUlsT3krUW1kVElTdy92aXNSeFkKNGt0YnJPYmY4WWRHYlJmcEJHYm44MEY4d1dENU9INnNwSXhBUEZ3RjdtalFKZUtWVE15S0EvcnZ0YkRjZnFmUQpNc3hiVTYzUlJ2aW9yd08xQmd5OVBUVXJRQkNnRGZTY2FrMzJldTNrakdFYnhjd1FPcTZtK2JJbkh0L1FKR2tRCk8vZHRDTWpjbW9nMWQrMG9tNHJRY3BXUU1LUlVyYklQZlFOdwotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg%3D%3D%22%2C%22server%22%3A%22https%3A//4FB5BA04C75599220274BBB942AE2DEA.gr7.us-east-1.eks.amazonaws.com%22%7D%2C%22name%22%3A%22polaris%22%7D%5D%2C%22contexts%22%3A%5B%7B%22context%22%3A%7B%22cluster%22%3A%22polaris%22%2C%22namespace%22%3A%22kpo-test%22%2C%22user%22%3A%22kpo-test%22%7D%2C%22name%22%3A%22kpo-test%22%7D%5D%2C%22current-context%22%3A%22kpo-test%22%2C%22kind%22%3A%22Config%22%2C%22preferences%22%3A%7B%7D%2C%22users%22%3A%5B%7B%22name%22%3A%22kpo-test%22%2C%22user%22%3A%7B%22token%22%3A%22eyJhbGciOiJSUzI1NiIsImtpZCI6IlJnUVBYQjNjcUlMY1VxZzVrUXQ4dlZyTkV6aDZUYl9oNFIwQmRYZFBBM3cifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJrcG8tdGVzdCIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VjcmV0Lm5hbWUiOiJrcG8tdGVzdC10b2tlbi1jN3F2NiIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50Lm5hbWUiOiJrcG8tdGVzdCIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50LnVpZCI6ImMyM2VkMmMxLTU4ZjgtNDdjYS04NjJmLWU3MDM4MzJiODUwYyIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDprcG8tdGVzdDprcG8tdGVzdCJ9.kSuNok2QQUvlYot5oznb0fAUuCBWYiee3mCCgdWquovJMNEPZkoMosLd_tzeReDih8mmLaY3BjXA-aOYAG_rXX7o0Bq_cpxPDcHCY1BbbTSkFIMm396-E4hAz54nuHkgPFLt2rg6_QTZ9sRz2w12ZaObniIilUKK5HL73BVm6Zg-pU4yKRlJma8Kz3zJQqR14AwXWrXu3v-GvPt6wTheW4x7P7PIn3p_F3SvFHhhkcVtqmzoh0eqZwg3sxpB_tDwye7zU-7hgmvNLjT3_qja1nDW7aePCUKk-13rHFJUpXGSKl0lD7b1uaElUWg7oDI8GPjX80s549fDAolQWEvObA%22%7D%7D%5D%7D%0A"
# ### </SETUP> ####

# import sys, urllib.parse.quote(yaml.dump(yaml.safe_load(open(sys.argv[1]))))

# DEV:


# PROD:
from kubernetes.client import models as k8s
from airflow.decorators import dag
from datetime import datetime

from include.providers.cncf.kubernetes.operators.multi_log_kubernetes_pod import MultiLogKubernetesPodOperator

# AIRFLOW_CONN_KUBERNETES_KPO='kubernetes://?extra__kubernetes__in_cluster=True'
# os.environ[]

@dag(
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False
)
def example_kpo_ml():
    MultiLogKubernetesPodOperator(
        # kubernetes_conn_id="kpo",
        full_pod_spec=k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                name="name"
            ),
            spec=k8s.V1PodSpec(
                init_containers=[k8s.V1Container(
                    name="kpo-test-init",
                    image='ubuntu',
                    command=["echo"],
                    args=["hi - kpo-test-init"]
                )],
                containers=[k8s.V1Container(
                    name="kpo-test",
                    image='ubuntu',
                    command=["echo"],
                    args=["hi - kpo-test"]
                ), k8s.V1Container(
                    name="kpo-test-sidecar",
                    image='ubuntu',
                    command=["echo"],
                    args=["hi - kpo-test-sidecar"]
                )],
            )
        ),
        task_id="kpo-test",
    )


example_kpo_dag = example_kpo_ml()
