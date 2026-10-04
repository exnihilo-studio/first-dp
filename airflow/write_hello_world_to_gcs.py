import pendulum
from airflow.decorators import dag, task

BUCKET_NAME = "gn-test-bucket"
# Only the data product's SA can write to the bucket. The Composer SA is allowed to
# impersonate it, so the task impersonates it instead of using the Composer SA directly.
DP_SERVICE_ACCOUNT = "first-dp-dev-composer-sa@gn-first-dp-dev-ens.iam.gserviceaccount.com"


@task()
def write_hello_world_to_gcs(**context) -> str:
    import google.auth
    from google.auth import impersonated_credentials
    from google.cloud import storage

    source_credentials, project_id = google.auth.default()
    target_credentials = impersonated_credentials.Credentials(
        source_credentials=source_credentials,
        target_principal=DP_SERVICE_ACCOUNT,
        target_scopes=["https://www.googleapis.com/auth/devstorage.read_write"],
    )

    object_name = f"hello_world/{context['ds_nodash']}/hello_world.txt"
    client = storage.Client(project=project_id, credentials=target_credentials)
    client.bucket(BUCKET_NAME).blob(object_name).upload_from_string(
        "hello world\n", content_type="text/plain"
    )

    uri = f"gs://{BUCKET_NAME}/{object_name}"
    print(f"Wrote {uri}")
    return uri


@dag(
    dag_id="write_hello_world_to_gcs",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    description="Writes hello world into a file on the data product's GCS bucket",
    schedule=None,
    catchup=False,
    tags=["genesis", "example", "gcs"],
)
def write_hello_world_to_gcs_dag():
    write_hello_world_to_gcs()


dag = write_hello_world_to_gcs_dag()
