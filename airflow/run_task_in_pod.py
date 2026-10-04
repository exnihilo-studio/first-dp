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
    # specify the Docker image to launch, it needs to be able to run a Python script
    image="europe-west2-docker.pkg.dev/gn-dev-ar-ens/first-dp",
    # launch the Pod on the same cluster as Airflow is running on
    in_cluster=True,
    # launch the Pod in the same namespace as Airflow is running in
    namespace="composer-user-workloads",
    # log events in case of Pod failure
    log_events_on_failure=True,
    # enable pushing to XCom
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
