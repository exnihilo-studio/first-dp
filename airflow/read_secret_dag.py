import pendulum
from airflow.decorators import dag, task

SECRETS_PROJECT = "gn-dev-kms-ens"
SECRET_NAME = "test-secret-01"
# The data product's SA has read access on the secrets project. The Composer SA is allowed to
# impersonate it, so the task impersonates it instead of using the Composer SA directly.
DP_SERVICE_ACCOUNT = "first-dp-dev-composer-sa@gn-first-dp-dev-ens.iam.gserviceaccount.com"


@task()
def print_secret_length() -> int:
    import google.auth
    from google.auth import impersonated_credentials
    from google.cloud import secretmanager

    source_credentials, _ = google.auth.default()
    target_credentials = impersonated_credentials.Credentials(
        source_credentials=source_credentials,
        target_principal=DP_SERVICE_ACCOUNT,
        target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    client = secretmanager.SecretManagerServiceClient(credentials=target_credentials)
    name = f"projects/{SECRETS_PROJECT}/secrets/{SECRET_NAME}/versions/latest"
    secret_value = client.access_secret_version(request={"name": name}).payload.data.decode("utf-8")

    # Only the length is logged, never the value itself.
    print(f"Secret {SECRET_NAME} has {len(secret_value)} characters")
    return len(secret_value)


@dag(
    dag_id="read_secret",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    description="Reads test-secret-01 from the secrets project and prints its length",
    schedule=None,
    catchup=False,
    tags=["genesis", "example", "secrets"],
)
def read_secret_dag():
    print_secret_length()


dag = read_secret_dag()
