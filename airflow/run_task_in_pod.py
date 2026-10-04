from datetime import datetime, timedelta
from airflow.decorators import dag, task

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 15),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


@task.kubernetes(
    image="europe-west2-docker.pkg.dev/gn-dev-ar-ens/first-dp/first-dp:v1.1.0-feat-run-task-in-pod.2",
    namespace="composer-user-workloads",
    kubernetes_conn_id="kubernetes_default",
    config_file="/home/airflow/composer_kube_config",
    in_cluster=False,
    log_events_on_failure=True,
    do_xcom_push=True,
)
def transform(data_point):
    multiplied_data_point = 23 * int(data_point)
    return multiplied_data_point


# Define the DAG
@dag(
    "k8s_operator_from_gcr",
    default_args=default_args,
    description="A DAG that uses KubernetesPodOperator with distroless Python container",
    schedule_interval=None,
    catchup=False,
    tags=["kubernetes", "gcr", "python"],
)
def k8s_operator_from_gcr():
    transform(3)


dag = k8s_operator_from_gcr()
